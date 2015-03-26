# Copyright (c) 2012 VMware, Inc.
# Copyright (c) 2011 Citrix Systems, Inc.
# Copyright 2011 OpenStack Foundation
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.
"""
Utility functions for Image transfer and manipulation.
"""
import contextlib
from lxml import etree
import os
import tarfile

from oslo.config import cfg
from oslo.vmware import image_transfer
from oslo.vmware.objects import datacenter as dc_obj
from oslo.vmware import rw_handles

from nova import exception
from nova.image import glance
from nova.openstack.common.gettextutils import _
from nova.openstack.common import log as logging
from nova.openstack.common import strutils
from nova.openstack.common import units
from nova.virt.vmwareapi import constants
from nova.virt.vmwareapi import read_write_util
from nova.virt.vmwareapi import vm_util
from nova.virt.vmwareapi import volumeops

vmware_images_opts = [
    cfg.BoolOpt('bypass_vcenter',
                default=False,
                help=_('If set to true, the images will be uploaded to the '
                       'datastore through ESX bypassing vCenter which gives '
                       'better performance. Should be set to False if the ESX '
                       'hosts are not accessible from the glance-api node or '
                       'vSphere version is less than 5.5 (2013 release).')),
]

CONF = cfg.CONF
CONF.register_opts(vmware_images_opts, group='vmware')

LOG = logging.getLogger(__name__)

LINKED_CLONE_PROPERTY = 'vmware_linked_clone'


class VMwareImage(object):
    def __init__(self, image_id,
                 file_size=0,
                 os_type=constants.DEFAULT_OS_TYPE,
                 adapter_type=constants.DEFAULT_ADAPTER_TYPE,
                 container_format=constants.DEFAULT_CONTAINER_FORMAT,
                 disk_type=constants.DEFAULT_DISK_TYPE,
                 file_type=constants.DEFAULT_DISK_FORMAT,
                 linked_clone=None,
                 locations=None,
                 vif_model=constants.DEFAULT_VIF_MODEL,
                 name=None):
        """VMwareImage holds values for use in building VMs.

            image_id (str): uuid of the image
            file_size (int): size of file in bytes
            os_type (str): name of guest os (use vSphere names only)
            adapter_type (str): name of the adapter's type
            disk_type (str): type of disk in thin, thick, etc
            file_type (str): vmdk or iso
            linked_clone(bool): use linked clone, or don't
        """
        self.image_id = image_id
        self.name = name
        self.file_size = file_size
        self.os_type = os_type
        self.adapter_type = adapter_type
        self.container_format = container_format
        self.disk_type = disk_type
        self.file_type = file_type
        self.locations = locations
        if locations is None:
            self.locations = []

        # Container type of OVA implies that disks have to be in
        # streamOptimized format.
        if self.container_format == constants.CONTAINER_FORMAT_OVA:
            self.disk_type = constants.DISK_TYPE_STREAM_OPTIMIZED

        # NOTE(vui): This should be removed when we restore the
        # descriptor-based validation.
        if (self.file_type is not None and
                self.file_type not in constants.DISK_FORMATS_ALL):
            raise exception.InvalidDiskFormat(disk_format=self.file_type)

        if linked_clone is not None:
            self.linked_clone = linked_clone
        else:
            self.linked_clone = CONF.vmware.use_linked_clone
        self.vif_model = vif_model

    @property
    def file_size_in_kb(self):
        return self.file_size / units.Ki

    @property
    def file_size_in_gb(self):
        return self.file_size / units.Gi

    @property
    def is_sparse(self):
        return self.disk_type == constants.DISK_TYPE_SPARSE

    @property
    def is_iso(self):
        return self.file_type == constants.DISK_FORMAT_ISO

    @property
    def vsphere_location(self):
        for loc in self.locations:
            loc_url = loc.get('url')
            if loc_url and loc_url.startswith('vsphere'):
                return loc_url

    @classmethod
    def from_image(cls, context, image_id, image_meta=None):
        """Returns VMwareImage, the subset of properties the driver uses.

        :param context: security context
        :param image_id - image id of image
        :param image_meta - image metadata we are working with
        :return: vmware image object
        :rtype: nova.virt.vmwareapi.vmware_images.VmwareImage
        """
        if image_meta is None:
            image_meta = {}

        properties = image_meta.get("properties", {})

        # calculate linked_clone flag, allow image properties to override the
        # global property set in the configurations.
        image_linked_clone = properties.get(LINKED_CLONE_PROPERTY,
                                            CONF.vmware.use_linked_clone)

        # catch any string values that need to be interpreted as boolean values
        linked_clone = strutils.bool_from_string(image_linked_clone)

        props = {
            'image_id': image_id,
            'linked_clone': linked_clone
        }

        if 'name' in image_meta:
            props['name'] = image_meta['name']
        if 'size' in image_meta:
            props['file_size'] = image_meta['size']
        if 'disk_format' in image_meta:
            props['file_type'] = image_meta['disk_format']
        if 'container_format' in image_meta:
            props['container_format'] = image_meta['container_format']
        if context and image_id:
            image_service = glance.get_default_image_service()
            image_locations = image_service._get_locations(context, image_id)
            props['locations'] = image_locations

        props_map = {
            'vmware_ostype': 'os_type',
            'vmware_adaptertype': 'adapter_type',
            'vmware_disktype': 'disk_type',
            'hw_vif_model': 'vif_model'
        }

        for k, v in props_map.iteritems():
            if k in properties:
                props[v] = properties[k]

        return cls(**props)


def upload_iso_to_datastore(iso_path, instance, **kwargs):
    LOG.debug("Uploading iso %s to datastore", iso_path,
              instance=instance)
    with open(iso_path, 'r') as iso_file:
        write_file_handle = read_write_util.VMwareHTTPWriteFile(
            kwargs.get("host"),
            kwargs.get("data_center_name"),
            kwargs.get("datastore_name"),
            kwargs.get("cookies"),
            kwargs.get("file_path"),
            os.fstat(iso_file.fileno()).st_size)

        LOG.debug("Uploading iso of size : %s ",
                  os.fstat(iso_file.fileno()).st_size)
        block_size = 0x10000
        data = iso_file.read(block_size)
        while len(data) > 0:
            write_file_handle.write(data)
            data = iso_file.read(block_size)
        write_file_handle.close()

    LOG.debug("Uploaded iso %s to datastore", iso_path,
              instance=instance)


def fetch_image(context, session, instance, host, port, dc_name, datastore,
                file_path, cookies=None):
    """Fetch image from Glance to ESX datastore."""
    LOG.debug("Downloading image file data %(image_ref)s to the ESX "
              "data store %(datastore_name)s",
              {'image_ref': instance['image_ref'],
               'datastore_name': datastore.name},
              instance=instance)
    instance_image_ref = instance['image_ref']
    (image_service, image_id) = glance.get_remote_image_service(
        context, instance_image_ref)
    metadata = image_service.show(context, image_id)
    image = image_service.download(context, image_id)
    # FIXME (arnaud): the datacenter attribute should be populated
    # at the construction of the datastore object.
    datacenter = dc_obj.Datacenter('FIXME', dc_name)
    datastore.datacenter = datacenter
    image_transfer.download_image(image, metadata, session, datastore,
                                  file_path, bypass=CONF.vmware.bypass_vcenter)
    LOG.debug("Downloaded image file data %(image_ref)s to "
              "%(file_path)s on the ESX data store "
              "%(datastore_name)s",
              {'image_ref': instance['image_ref'],
               'file_path': file_path,
               'datastore_name': datastore.name},
              instance=instance)


def get_disk_info_from_ovf(xmlstr):
    ovf = etree.fromstring(xmlstr)
    nsovf = "{%s}" % ovf.nsmap["ovf"]
    nsrasd = "{%s}" % ovf.nsmap["rasd"]

    disk = ovf.find("./%sDiskSection/%sDisk" % (nsovf, nsovf))
    disk_id = disk.get("%sdiskId" % nsovf)
    file_id = disk.get("%sfileRef" % nsovf)

    file = ovf.find('./%sReferences/%sFile[@%sid="%s"]' % (nsovf, nsovf,
                                                           nsovf, file_id))
    vmdk_name = file.get("%shref" % nsovf)

    hrsrcs = ovf.findall(".//%sHostResource" % (nsrasd))
    hrsrc = [x for x in hrsrcs if x.text == "ovf:/disk/%s" % disk_id][0]
    item = hrsrc.getparent()
    controller_id = item.find("%sParent" % nsrasd).text

    adapter_type = "busLogic"

    instance_nodes = ovf.findall(".//%sItem/%sInstanceID" % (nsovf, nsrasd))
    instance_node = [x for x in instance_nodes if x.text == controller_id][0]
    item = instance_node.getparent()
    desc = item.find("%sDescription" % nsrasd)
    if desc and desc.text == "IDE Controller":
        adapter_type = "ide"
    else:
        sub_type = item.find("%sResourceSubType")
        if sub_type:
            adapter_type = sub_type.text

    return (vmdk_name, adapter_type)


def _build_import_spec_for_import_vapp(
        session, vm_name, datastore_name):
    dummy_disk_kb = 0
    vm_create_spec = volumeops.get_import_vapp_create_spec(
            session, vm_name, dummy_disk_kb, "thin", datastore_name)

    client_factory = session._get_vim().client.factory
    vm_import_spec = client_factory.create(
                             'ns0:VirtualMachineImportSpec')
    vm_import_spec.configSpec = vm_create_spec
    return vm_import_spec


def fetch_ova_image(
        context, instance, session, vm_name, datastore_name, vm_folder_ref,
        res_pool_ref):
    """Download and process OVA image from Glance server.

    The OVA is downloaded, and its OVF file extracted and parsed for the first
    disk. The data of said disk (in streamOptimized format) is in turn
    transfered to the hypervisor. The size of the first disk is returned.
    """

    instance_image_ref = instance['image_ref']
    (image_service, image_id) = glance.get_remote_image_service(
                                        context,
                                        instance_image_ref)
    LOG.debug("Downloading image %s from glance image server", image_id)
    read_iter = image_service.download(context, image_id)
    read_handle = rw_handles.ImageReadHandle(read_iter)

    with contextlib.closing(
            tarfile.open(mode="r|", fileobj=read_handle)) as tar:
        vmdk_name = None
        for tar_info in tar:
            if tar_info:
                if tar_info.name.endswith(".ovf"):
                    extracted = tar.extractfile(tar_info)
                    xmlstr = extracted.read()
                    (vmdk_name,
                     adapter_type) = get_disk_info_from_ovf(xmlstr)

                    # TODO(vui): Consider overriding the adapter type used
                    # in spawn with adapter_type, regardless of the value
                    # of the 'vmware_adaptertype' image property
                    LOG.debug("Extracted disk's adapter type is %s",
                              adapter_type)
                elif vmdk_name and tar_info.name.startswith(vmdk_name):
                    # Actual file name is <vmdk_name>.dddddddd
                    extracted = tar.extractfile(tar_info)

                    vm_import_spec = _build_import_spec_for_import_vapp(
                            session, vm_name, datastore_name)

                    imported_vm_ref = (
                        image_transfer.download_stream_optimized_data(
                            context,
                            CONF.vmware.image_transfer_timeout_secs,
                            extracted,
                            session=session,
                            host=session._host,
                            port=session._port,
                            image_size=tar_info.size,
                            resource_pool=res_pool_ref,
                            vm_folder=vm_folder_ref,
                            vm_import_spec=vm_import_spec))
                    extracted.close()
                    vmdk = vm_util.get_vmdk_info(session,
                                                 imported_vm_ref,
                                                 vm_name)
                    try:
                        LOG.debug("Unregistering the VM",
                                  instance=instance)
                        session._call_method(session._get_vim(),
                                             "UnregisterVM",
                                             imported_vm_ref)
                    except Exception as excep:
                        LOG.warn("Exception while unregistering the "
                                 "imported VM: %s" % str(excep))
                    return vmdk.capacity_in_bytes
        raise exception.ImageUnacceptable(
            reason=_("Extracting vmdk from OVA failed."),
            image_id=image_id)


def fetch_image_stream_optimized(
        context, instance, session, vm_name, datastore_name, vm_folder_ref,
        res_pool_ref):
    """Fetch image from Glance to ESX datastore.

    Returns the size of the virtual disk created from this image.
    """
    LOG.debug("Downloading image file data %(image_ref)s to the ESX "
              "as VM named '%(vm_name)s'",
              {'image_ref': instance['image_ref'],
               'vm_name': vm_name},
              instance=instance)
    instance_image_ref = instance['image_ref']
    (image_service, image_id) = glance.get_remote_image_service(
                                        context,
                                        instance_image_ref)
    metadata = image_service.show(context, image_id)
    image_size = int(metadata['size'])

    vm_import_spec = _build_import_spec_for_import_vapp(
            session, vm_name, datastore_name)

    imported_vm_ref = image_transfer.download_stream_optimized_image(
        context,
        CONF.vmware.image_transfer_timeout_secs,
        image_service,
        image_id,
        session=session,
        host=session._host,
        port=session._port,
        image_size=image_size,
        resource_pool=res_pool_ref,
        vm_folder=vm_folder_ref,
        vm_import_spec=vm_import_spec)
    LOG.debug("Downloaded image file data %(image_ref)s",
              {'image_ref': instance['image_ref']},
              instance=instance)
    vmdk = vm_util.get_vmdk_info(session, imported_vm_ref, vm_name)
    try:
        LOG.debug("Unregistering the VM", instance=instance)
        session._call_method(session._get_vim(),
                             "UnregisterVM", imported_vm_ref)
    except Exception as excep:
        LOG.warn("Exception while unregistering the "
                 "imported VM: %s" % str(excep))
    return vmdk.capacity_in_bytes


def upload_image_stream_optimized(context, image, instance, session,
                                  vm, os_type, adapter_type, vmdk_size,
                                  data_center_name, datastore_name):
    (image_service, image_id) = glance.get_remote_image_service(context,
                                                                image)
    metadata = image_service.show(context, image_id)

    cookies = session._get_vim().client.options.transport.cookiejar
    LOG.debug("Uploading image %s" % image_id, instance=instance)
    metadata = image_service.show(context, image_id)

    # TODO(vui): provide adapter number + device unit number to select
    #            root disk in multi-disk scenario.
    image_transfer.upload_image(
        context,
        CONF.vmware.image_transfer_timeout_secs,
        image_service,
        image_id,
        instance['project_id'],
        vm=vm,
        os_type=os_type,
        adapter_type=adapter_type,
        image_version=1,
        is_public=metadata['is_public'],
        vmdk_size=vmdk_size,
        image_name=metadata['name'],
        host=session._host,
        data_center_name=data_center_name,
        datastore_name=datastore_name,
        cookies=cookies,
        session=session)

    LOG.debug("Uploaded image %s" % image_id, instance=instance)
