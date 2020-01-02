###########################################################################
#
#  Copyright 2020 Google Inc.
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

from orchestra.google.marketing_platform.utils.schema.sdf.v5 import (
  Ad,
  AdGroup,
  Campaign,
  InsertionOrder,
  LineItem
)


SDF_V5_SCHEMA_TYPES = {
    'LINE_ITEM': LineItem.SDF_LineItem_Schema,
    'CAMPAIGN': Campaign.SDF_Campaign_Schema,
    'AD': Ad.SDF_Ad_Schema,
    'AD_GROUP': AdGroup.SDF_AdGroup_Schema,
    'INSERTION_ORDER': InsertionOrder.SDF_InsertionOrder_Schema
}