# Copyright 2013 Intel Corporation
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

from sqlalchemy import Column
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy import String


def upgrade(migrate_engine):
    meta = MetaData()
    meta.bind = migrate_engine

    # Add a new column metrics to save metrics info for compute nodes
    instance_group = Table('instance_groups', meta, autoload=True)
    shadow_instance_group = Table('shadow_instance_groups', meta, autoload=True)

    description = Column('description', String(length=255), nullable=True)
    shadow_description = Column('description', String(length=255), nullable=True)
    instance_group.create_column(description)
    shadow_instance_group.create_column(shadow_description)


def downgrade(migrate_engine):
    meta = MetaData()
    meta.bind = migrate_engine

    # Remove the new column
    instance_group = Table('instance_groups', meta, autoload=True)
    shadow_instance_group = Table('shadow_instance_groups', meta, autoload=True)

    instance_group.drop_column('description')
    shadow_instance_group.drop_column('description')
