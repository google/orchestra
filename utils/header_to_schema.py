###########################################################################
#
#  Copyright 2018 Google Inc.
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


import re
from Lookup import DCM_Field_Lookup

RE_HUMAN = re.compile('[^0-9a-zA-Z]+')

def column_header_sanitize(cell):
  return RE_HUMAN.sub('_', cell.title()).strip('_')


def report_schema(headers):
  schema = []

  for header_name in headers:
    header_sanitized = column_header_sanitize(header_name)

    # first try exact match
    header_type = DCM_Field_Lookup.get(header_sanitized)

    if header_type is None:
        header_type = DCM_Field_Lookup.get('Dbm_' + header_sanitized)
    # second try to match end for custom field names ( activity reports )
    if header_type is None:
      for field_name, field_type in DCM_Field_Lookup.items():
        if header_sanitized.endswith('_' + field_name):
          header_type = field_type
          break

    # finally default it to STRING
    if header_type is None: header_type = 'STRING'
    if header_sanitized is "Revenue_Adv_Currency":
      header_sanitized = "Revenue__Adv_Currency_"
    schema.append({
      'name':header_sanitized,
      'type':header_type,
      'mode':'NULLABLE'
    })

  return schema
