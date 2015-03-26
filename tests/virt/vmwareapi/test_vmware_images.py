# Copyright (c) 2014 VMware, Inc.
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
Test suite for vmware_images.
"""

import contextlib

import mock
from oslo.vmware import image_transfer
from oslo.vmware.objects import datastore as ds_obj

from nova import exception
from nova.network import model as network_model
from nova.openstack.common import units
from nova import test
import nova.tests.image.fake
from nova.virt.vmwareapi import constants
from nova.virt.vmwareapi import vm_util
from nova.virt.vmwareapi import vmware_images
from nova.virt.vmwareapi import volumeops


class VMwareImagesTestCase(test.NoDBTestCase):
    """Unit tests for Vmware API connection calls."""

    def setUp(self):
        super(VMwareImagesTestCase, self).setUp()
        self._transfer_timeout_secs = 1
        self.flags(image_transfer_timeout_secs=self._transfer_timeout_secs,
                   group='vmware')

    def tearDown(self):
        super(VMwareImagesTestCase, self).tearDown()

    def test_fetch_image(self):
        """Test fetching images."""

        dc_name = 'fake-dc'
        file_path = 'fake_file'
        ds = ds_obj.Datastore('fake-ref', 'ds1')
        host = mock.MagicMock()
        port = mock.MagicMock()
        context = mock.MagicMock()
        session = mock.MagicMock()
        cookies = mock.MagicMock()
        image = mock.MagicMock()

        image_data = {
                'id': nova.tests.image.fake.get_valid_image_id(),
                'disk_format': 'vmdk',
                'size': 512,
            }
        instance = {}
        instance['image_ref'] = image_data['id']
        instance['uuid'] = 'fake-uuid'

        with contextlib.nested(
             mock.patch.object(nova.image.glance, "get_remote_image_service"),
             mock.patch.object(image_transfer, 'download_image')
        ) as (mock_get_remote_image_service, mock_download_image):
            mock_image_service = mock.MagicMock()
            mock_image_service.show.return_value = image_data
            mock_image_service.download.return_value = image
            mock_get_remote_image_service.return_value = (mock_image_service,
                                                          image_data['id'])

            vmware_images.fetch_image(
                    context, session, instance, host, port, dc_name, ds,
                    file_path, cookies)

            mock_image_service.show.assert_called_once_with(
                    context, instance['image_ref'])
            mock_download_image.assert_called_once_with(
                    image, image_data, session, ds, file_path, bypass=False)

    def test_fetch_image_stream_optimized(self):
        """Test fetching streamOptimized disk image."""

        vm_name = 'fake-vm-name'
        ds_name = 'fake-ds-name'
        vm_folder_ref = mock.MagicMock()
        res_pool_ref = mock.MagicMock()
        context = mock.MagicMock()
        session = mock.MagicMock()
        fake_vm_ref = mock.MagicMock()
        vm_import_spec = mock.ANY  # FIXME

        image_data = {
                'id': nova.tests.image.fake.get_valid_image_id(),
                'disk_format': 'vmdk',
                'size': 512,
            }
        instance = {}
        instance['image_ref'] = image_data['id']
        instance['uuid'] = 'fake-uuid'

        def fake_call_method(module, method, *args, **kwargs):
            self.assertEqual('XUnregisterVM', method)
            self.assertEqual([fake_vm_ref], args)

        with contextlib.nested(
             mock.patch.object(nova.image.glance, 'get_remote_image_service'),
             mock.patch.object(image_transfer,
                 'download_stream_optimized_image', return_value=fake_vm_ref),
             mock.patch.object(volumeops, 'get_import_vapp_create_spec'),
             mock.patch.object(session, '_call_method'),
             mock.patch.object(vm_util, 'get_vmdk_info')
        ) as (mock_get_remote_image_service,
              mock_download_stream_optimized_image,
              mock_get_import_vapp_create_spec,
              mock_call_method,
              mock_get_vmdk_info):
            mock_image_service = mock.MagicMock()
            mock_image_service.show.return_value = image_data
            mock_get_remote_image_service.return_value = (mock_image_service,
                                                          image_data['id'])
            vmware_images.fetch_image_stream_optimized(
                    context, instance, session, vm_name, ds_name,
                    vm_folder_ref, res_pool_ref)

            mock_image_service.show.assert_called_once_with(
                    context, instance['image_ref'])
            mock_download_stream_optimized_image.assert_called_once_with(
                    context,
                    self._transfer_timeout_secs,
                    mock_image_service,
                    image_data['id'],
                    session=session,
                    host=session._host,
                    port=session._port,
                    image_size=image_data['size'],
                    resource_pool=res_pool_ref,
                    vm_folder=vm_folder_ref,
                    vm_import_spec=vm_import_spec)
            mock_get_vmdk_info.assert_called_once_with(session,
                                                       fake_vm_ref,
                                                       vm_name)
            mock_call_method.assert_called_once_with(
                    session._get_vim.return_value, "UnregisterVM", fake_vm_ref)

    def test_upload_image_stream_optimized(self):
        # TODO(vui) add test to exercise upload_image_stream_optimized()
        pass

    def test_fetch_ova_image(self):
        # TODO(vui) add test to exercise fetch_ova_image()
        pass

    def test_get_image_properties_with_image_ref(self):
        raw_disk_size_in_gb = 83
        raw_disk_size_in_bytes = raw_disk_size_in_gb * units.Gi
        image_id = nova.tests.image.fake.get_valid_image_id()
        mdata = {'size': raw_disk_size_in_bytes,
                 'disk_format': 'vmdk',
                 'properties': {
                     "vmware_ostype": constants.DEFAULT_OS_TYPE,
                     "vmware_adaptertype": constants.DEFAULT_ADAPTER_TYPE,
                     "vmware_disktype": constants.DEFAULT_DISK_TYPE,
                     "hw_vif_model": constants.DEFAULT_VIF_MODEL,
                     vmware_images.LINKED_CLONE_PROPERTY: True}}

        img_props = vmware_images.VMwareImage.from_image(None, image_id, mdata)

        image_size_in_kb = raw_disk_size_in_bytes / units.Ki
        image_size_in_gb = raw_disk_size_in_bytes / units.Gi

        # assert that defaults are set and no value returned is left empty
        self.assertEqual(constants.DEFAULT_OS_TYPE, img_props.os_type)
        self.assertEqual(constants.DEFAULT_ADAPTER_TYPE,
                         img_props.adapter_type)
        self.assertEqual(constants.DEFAULT_DISK_TYPE, img_props.disk_type)
        self.assertEqual(constants.DEFAULT_VIF_MODEL, img_props.vif_model)
        self.assertTrue(img_props.linked_clone)
        self.assertEqual(image_size_in_kb, img_props.file_size_in_kb)
        self.assertEqual(image_size_in_gb, img_props.file_size_in_gb)

    def _image_build(self, image_lc_setting, global_lc_setting,
                     disk_format=constants.DEFAULT_DISK_FORMAT):
        self.flags(use_linked_clone=global_lc_setting, group='vmware')
        raw_disk_size_in_gb = 93
        raw_disk_size_in_btyes = raw_disk_size_in_gb * units.Gi

        image_id = nova.tests.image.fake.get_valid_image_id()
        mdata = {'size': raw_disk_size_in_btyes,
                 'disk_format': disk_format,
                 'properties': {
                     "vmware_ostype": constants.DEFAULT_OS_TYPE,
                     "vmware_adaptertype": constants.DEFAULT_ADAPTER_TYPE,
                     "vmware_disktype": constants.DEFAULT_DISK_TYPE,
                     "hw_vif_model": constants.DEFAULT_VIF_MODEL}}

        if image_lc_setting is not None:
            mdata['properties'][
                vmware_images.LINKED_CLONE_PROPERTY] = image_lc_setting

        return vmware_images.VMwareImage.from_image(None, image_id, mdata)

    def test_use_linked_clone_override_nf(self):
        image_props = self._image_build(None, False)
        self.assertFalse(image_props.linked_clone,
                         "No overrides present but still overridden!")

    def test_use_linked_clone_override_nt(self):
        image_props = self._image_build(None, True)
        self.assertTrue(image_props.linked_clone,
                        "No overrides present but still overridden!")

    def test_use_linked_clone_override_ny(self):
        image_props = self._image_build(None, "yes")
        self.assertTrue(image_props.linked_clone,
                        "No overrides present but still overridden!")

    def test_use_linked_clone_override_ft(self):
        image_props = self._image_build(False, True)
        self.assertFalse(image_props.linked_clone,
                         "image level metadata failed to override global")

    def test_use_linked_clone_override_string_nt(self):
        image_props = self._image_build("no", True)
        self.assertFalse(image_props.linked_clone,
                         "image level metadata failed to override global")

    def test_use_linked_clone_override_string_yf(self):
        image_props = self._image_build("yes", False)
        self.assertTrue(image_props.linked_clone,
                        "image level metadata failed to override global")

    def test_use_disk_format_none(self):
        image = self._image_build(None, True, None)
        self.assertIsNone(image.file_type)
        self.assertFalse(image.is_iso)

    def test_use_disk_format_iso(self):
        image = self._image_build(None, True, 'iso')
        self.assertEqual(image.file_type, 'iso')
        self.assertTrue(image.is_iso)

    def test_use_bad_disk_format(self):
        self.assertRaises(exception.InvalidDiskFormat,
                          self._image_build,
                          None,
                          True,
                          "bad_disk_format")

    def test_image_defaults(self):
        image = vmware_images.VMwareImage(image_id='fake-image-id')

        # N.B. We intentially don't use the defined constants here. Amongst
        # other potential failures, we're interested in changes to their
        # values, which would not otherwise be picked up.
        self.assertEqual(image.os_type, 'otherGuest')
        self.assertEqual(image.adapter_type, 'lsiLogic')
        self.assertEqual(image.disk_type, 'preallocated')
        self.assertEqual(image.vif_model, network_model.VIF_MODEL_E1000)
