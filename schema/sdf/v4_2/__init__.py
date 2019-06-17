###########################################################################
#
#  Copyright 2019 Google Inc.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
###########################################################################

from Ad import SDF_Ad_Schema
from AdGroup import SDF_AdGroup_Schema
from Campaign import SDF_Campaign_Schema
from InsertionOrder import SDF_InsertionOrder_Schema
from LineItem import SDF_LineItem_Schema

SDF_V4_2_SCHEMA_TYPES = {
    'LINE_ITEM': SDF_LineItem_Schema,
    'CAMPAIGN': SDF_Campaign_Schema,
    'AD': SDF_Ad_Schema,
    'AD_GROUP': SDF_AdGroup_Schema,
    'INSERTION_ORDER': SDF_InsertionOrder_Schema
}
