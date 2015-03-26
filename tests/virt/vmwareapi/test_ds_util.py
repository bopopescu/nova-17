# Copyright (c) 2014 VMware, Inc.
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

import contextlib
import re

import mock
from oslo.vmware import exceptions as vexc
from oslo.vmware.objects import datastore as ds_obj

from nova import exception
from nova.openstack.common.gettextutils import _
from nova.openstack.common import units
from nova import test
from nova.tests.virt.vmwareapi import fake
from nova.virt.vmwareapi import ds_util


class DsUtilTestCase(test.NoDBTestCase):
    def setUp(self):
        super(DsUtilTestCase, self).setUp()
        self.session = fake.FakeSession()
        self.flags(api_retry_count=1, group='vmware')
        fake.reset()

    def tearDown(self):
        super(DsUtilTestCase, self).tearDown()
        fake.reset()

    def test_file_delete(self):
        def fake_call_method(module, method, *args, **kwargs):
            self.assertEqual('DeleteDatastoreFile_Task', method)
            name = kwargs.get('name')
            self.assertEqual('[ds] fake/path', name)
            datacenter = kwargs.get('datacenter')
            self.assertEqual('fake-dc-ref', datacenter)
            return 'fake_delete_task'

        with contextlib.nested(
            mock.patch.object(self.session, '_wait_for_task'),
            mock.patch.object(self.session, '_call_method',
                              fake_call_method)
        ) as (_wait_for_task, _call_method):
            ds_path = ds_obj.DatastorePath('ds', 'fake/path')
            ds_util.file_delete(self.session,
                                ds_path, 'fake-dc-ref')
            _wait_for_task.assert_has_calls([
                   mock.call('fake_delete_task')])

    def test_file_move(self):
        def fake_call_method(module, method, *args, **kwargs):
            self.assertEqual('MoveDatastoreFile_Task', method)
            sourceName = kwargs.get('sourceName')
            self.assertEqual('[ds] tmp/src', sourceName)
            destinationName = kwargs.get('destinationName')
            self.assertEqual('[ds] base/dst', destinationName)
            sourceDatacenter = kwargs.get('sourceDatacenter')
            self.assertEqual('fake-dc-ref', sourceDatacenter)
            destinationDatacenter = kwargs.get('destinationDatacenter')
            self.assertEqual('fake-dc-ref', destinationDatacenter)
            return 'fake_move_task'

        with contextlib.nested(
            mock.patch.object(self.session, '_wait_for_task'),
            mock.patch.object(self.session, '_call_method',
                              fake_call_method)
        ) as (_wait_for_task, _call_method):
            src_ds_path = ds_obj.DatastorePath('ds', 'tmp/src')
            dst_ds_path = ds_obj.DatastorePath('ds', 'base/dst')
            ds_util.file_move(self.session,
                              'fake-dc-ref', src_ds_path, dst_ds_path)
            _wait_for_task.assert_has_calls([
                   mock.call('fake_move_task')])

    def test_disk_move(self):
        def fake_call_method(module, method, *args, **kwargs):
            self.assertEqual('MoveVirtualDisk_Task', method)
            sourceName = kwargs.get('sourceName')
            self.assertEqual('[ds] tmp/src', sourceName)
            destName = kwargs.get('destName')
            self.assertEqual('[ds] base/dst', destName)
            sourceDatacenter = kwargs.get('sourceDatacenter')
            self.assertEqual('fake-dc-ref', sourceDatacenter)
            destDatacenter = kwargs.get('destDatacenter')
            self.assertEqual('fake-dc-ref', destDatacenter)
            return 'fake_move_task'

        with contextlib.nested(
            mock.patch.object(self.session, '_wait_for_task'),
            mock.patch.object(self.session, '_call_method',
                              fake_call_method)
        ) as (_wait_for_task, _call_method):
            ds_util.disk_move(self.session,
                              'fake-dc-ref', '[ds] tmp/src', '[ds] base/dst')
            _wait_for_task.assert_has_calls([
                   mock.call('fake_move_task')])

    def test_disk_copy(self):
        def fake_call_method(module, method, *args, **kwargs):
            self.assertEqual('CopyVirtualDisk_Task', method)
            src_name = kwargs.get('sourceName')
            self.assertEqual('[ds] tmp/src.vmdk', src_name)
            dest_name = kwargs.get('destName')
            self.assertEqual('[ds] tmp/dst.vmdk', dest_name)
            src_datacenter = kwargs.get('sourceDatacenter')
            self.assertEqual('fake-dc-ref', src_datacenter)
            dest_datacenter = kwargs.get('destDatacenter')
            self.assertEqual('fake-dc-ref', dest_datacenter)
            return 'fake_copy_task'

        with contextlib.nested(
            mock.patch.object(self.session, '_wait_for_task'),
            mock.patch.object(self.session, '_call_method',
                              fake_call_method)
        ) as (_wait_for_task, _call_method):
            ds_util.disk_copy(self.session,
                              'fake-dc-ref', '[ds] tmp/src.vmdk',
                              '[ds] tmp/dst.vmdk')
            _wait_for_task.assert_has_calls([
                   mock.call('fake_copy_task')])

    def test_disk_delete(self):
        def fake_call_method(module, method, *args, **kwargs):
            self.assertEqual('DeleteVirtualDisk_Task', method)
            name = kwargs.get('name')
            self.assertEqual('[ds] tmp/disk.vmdk', name)
            datacenter = kwargs.get('datacenter')
            self.assertEqual('fake-dc-ref', datacenter)
            return 'fake_delete_task'

        with contextlib.nested(
            mock.patch.object(self.session, '_wait_for_task'),
            mock.patch.object(self.session, '_call_method',
                              fake_call_method)
        ) as (_wait_for_task, _call_method):
            ds_util.disk_delete(self.session,
                                'fake-dc-ref', '[ds] tmp/disk.vmdk')
            _wait_for_task.assert_has_calls([
                   mock.call('fake_delete_task')])

    def test_mkdir(self):
        def fake_call_method(module, method, *args, **kwargs):
            self.assertEqual('MakeDirectory', method)
            name = kwargs.get('name')
            self.assertEqual('[ds] fake/path', name)
            datacenter = kwargs.get('datacenter')
            self.assertEqual('fake-dc-ref', datacenter)
            createParentDirectories = kwargs.get('createParentDirectories')
            self.assertTrue(createParentDirectories)

        with mock.patch.object(self.session, '_call_method',
                               fake_call_method):
            ds_path = ds_obj.DatastorePath('ds', 'fake/path')
            ds_util.mkdir(self.session, ds_path, 'fake-dc-ref')

    def test_file_exists(self):
        def fake_call_method(module, method, *args, **kwargs):
            if method == 'SearchDatastore_Task':
                ds_browser = args[0]
                self.assertEqual('fake-browser', ds_browser)
                datastorePath = kwargs.get('datastorePath')
                self.assertEqual('[ds] fake/path', datastorePath)
                return 'fake_exists_task'

            # Should never get here
            self.fail()

        def fake_wait_for_task(task_ref):
            if task_ref == 'fake_exists_task':
                result_file = fake.DataObject()
                result_file.path = 'fake-file'

                result = fake.DataObject()
                result.file = [result_file]
                result.path = '[ds] fake/path'

                task_info = fake.DataObject()
                task_info.result = result

                return task_info

            # Should never get here
            self.fail()

        with contextlib.nested(
                mock.patch.object(self.session, '_call_method',
                                  fake_call_method),
                mock.patch.object(self.session, '_wait_for_task',
                                  fake_wait_for_task)):
            ds_path = ds_obj.DatastorePath('ds', 'fake/path')
            file_exists = ds_util.file_exists(self.session,
                    'fake-browser', ds_path, 'fake-file')
            self.assertTrue(file_exists)

    def test_file_exists_fails(self):
        def fake_call_method(module, method, *args, **kwargs):
            if method == 'SearchDatastore_Task':
                return 'fake_exists_task'

            # Should never get here
            self.fail()

        def fake_wait_for_task(task_ref):
            if task_ref == 'fake_exists_task':
                raise vexc.FileNotFoundException()

            # Should never get here
            self.fail()

        with contextlib.nested(
                mock.patch.object(self.session, '_call_method',
                                  fake_call_method),
                mock.patch.object(self.session, '_wait_for_task',
                                  fake_wait_for_task)):
            ds_path = ds_obj.DatastorePath('ds', 'fake/path')
            file_exists = ds_util.file_exists(self.session,
                    'fake-browser', ds_path, 'fake-file')
            self.assertFalse(file_exists)

    def test_get_datastore(self):
        fake_objects = fake.FakeRetrieveResult()
        fake_objects.add_object(fake.Datastore())
        result = ds_util.get_datastore(
            fake.FakeObjectRetrievalSession(fake_objects))

        self.assertEqual("fake-ds", result.name)
        self.assertEqual(units.Ti, result.capacity)
        self.assertEqual(500 * units.Gi, result.freespace)

    def test_get_datastore_with_regex(self):
        # Test with a regex that matches with a datastore
        datastore_valid_regex = re.compile("^openstack.*\d$")
        fake_objects = fake.FakeRetrieveResult()
        fake_objects.add_object(fake.Datastore("openstack-ds0"))
        fake_objects.add_object(fake.Datastore("fake-ds0"))
        fake_objects.add_object(fake.Datastore("fake-ds1"))
        result = ds_util.get_datastore(
            fake.FakeObjectRetrievalSession(fake_objects), None, None,
            datastore_valid_regex)
        self.assertEqual("openstack-ds0", result.name)

    def test_get_datastore_with_token(self):
        regex = re.compile("^ds.*\d$")
        fake0 = fake.FakeRetrieveResult()
        fake0.add_object(fake.Datastore("ds0", 10 * units.Gi, 5 * units.Gi))
        fake0.add_object(fake.Datastore("foo", 10 * units.Gi, 9 * units.Gi))
        setattr(fake0, 'token', 'token-0')
        fake1 = fake.FakeRetrieveResult()
        fake1.add_object(fake.Datastore("ds2", 10 * units.Gi, 8 * units.Gi))
        fake1.add_object(fake.Datastore("ds3", 10 * units.Gi, 1 * units.Gi))
        result = ds_util.get_datastore(
            fake.FakeObjectRetrievalSession(fake0, fake1), None, None, regex)
        self.assertEqual("ds2", result.name)

    def test_get_datastore_with_list(self):
        # Test with a regex containing whitelist of datastores
        datastore_valid_regex = re.compile("(openstack-ds0|openstack-ds2)")
        fake_objects = fake.FakeRetrieveResult()
        fake_objects.add_object(fake.Datastore("openstack-ds0"))
        fake_objects.add_object(fake.Datastore("openstack-ds1"))
        fake_objects.add_object(fake.Datastore("openstack-ds2"))
        result = ds_util.get_datastore(
            fake.FakeObjectRetrievalSession(fake_objects), None, None,
            datastore_valid_regex)
        self.assertNotEqual("openstack-ds1", result.name)

    def test_get_datastore_with_regex_error(self):
        # Test with a regex that has no match
        # Checks if code raises DatastoreNotFound with a specific message
        datastore_invalid_regex = re.compile("unknown-ds")
        exp_message = (_("Datastore regex %s did not match any datastores")
                       % datastore_invalid_regex.pattern)
        fake_objects = fake.FakeRetrieveResult()
        fake_objects.add_object(fake.Datastore("fake-ds0"))
        fake_objects.add_object(fake.Datastore("fake-ds1"))
        # assertRaisesRegExp would have been a good choice instead of
        # try/catch block, but it's available only from Py 2.7.
        try:
            ds_util.get_datastore(
                fake.FakeObjectRetrievalSession(fake_objects), None, None,
                datastore_invalid_regex)
        except exception.DatastoreNotFound as e:
            self.assertEqual(exp_message, e.args[0])
        else:
            self.fail("DatastoreNotFound Exception was not raised with "
                      "message: %s" % exp_message)

    def test_get_datastore_without_datastore(self):

        self.assertRaises(exception.DatastoreNotFound,
                ds_util.get_datastore,
                fake.FakeObjectRetrievalSession(None), host="fake-host")

        self.assertRaises(exception.DatastoreNotFound,
                ds_util.get_datastore,
                fake.FakeObjectRetrievalSession(None), cluster="fake-cluster")

    def test_get_datastore_no_host_in_cluster(self):
        self.assertRaises(exception.DatastoreNotFound,
                          ds_util.get_datastore,
                          fake.FakeObjectRetrievalSession(""), 'fake_cluster')

    def test_get_datastore_inaccessible_ds(self):
        data_store = fake.Datastore()
        data_store.set("summary.accessible", False)

        fake_objects = fake.FakeRetrieveResult()
        fake_objects.add_object(data_store)

        self.assertRaises(exception.DatastoreNotFound,
                          ds_util.get_datastore,
                          fake.FakeObjectRetrievalSession(fake_objects))

    def test_get_datastore_by_name(self):
        ds_refs = []
        ds_ref = ds_util.get_datastore_by_name(self.session,
                                               ds_refs, 'fake')
        self.assertIsNone(ds_ref)

        ds_refs = ['fake-ref1', 'fake-ref2']
        ds = ds_obj.Datastore('fake-refs1', 'fake')
        with mock.patch.object(ds_util, 'get_datastore_by_ref',
                               return_value=ds) as mock_get:
            ds_ref = ds_util.get_datastore_by_name(self.session,
                                                   ds_refs, 'fake')
            self.assertEqual('fake-ref1', ds_ref)

        ds_refs = ['fake-ref1', 'fake-ref2']
        ds = ds_obj.Datastore('fake-refs', 'fake-nomatch')
        with mock.patch.object(ds_util, 'get_datastore_by_ref',
                               return_value=ds) as mock_get:
            ds_ref = ds_util.get_datastore_by_name(self.session,
                                                   ds_refs, 'fake')
            self.assertIsNone(ds_ref)
