#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import json
import os
from random import randint
import tempfile
import time
from hooks.gcs_hook import GoogleCloudStorageHook



def json_to_jsonlines(json_file):
  """Naive Implentation for json to jsonlines.

  Args:
    json_file: A string, the file name of the json file.
  Returns:
    The input json file as new line delimited json.
  """
  with open(json_file) as f:
    data = json.load(f)
  return '\n'.join([json.dumps(d) for d in data])


def download_and_transform_erf(self, partner_id=None):
  """Load and Transform ERF files to Newline Delimeted JSON.

  Then upload this file to the project GCS.

  Args:
    self: The operator this is being used in.
    partner_id: A string of the DCM id of the partner.

  Returns:
    entity_read_file_ndj: The filename for the converted entity read file.
  """
  if partner_id:
    self.erf_bucket = 'gdbm-%s' % partner_id
  else:
    self.erf_bucket = 'gdbm-public'

  gcs_hook = GoogleCloudStorageHook(google_cloud_storage_conn_id=self.conn_id)
  entity_read_file = tempfile.NamedTemporaryFile(delete=False)
  gcs_hook.download(self.erf_bucket, self.erf_object, entity_read_file.name)
  temp_file = None
  # Creating temp file. Not using the delete-on-close functionality
  # as opening the file for reading while still open for writing
  # will not work on all platform
  # https://docs.python.org/2/library/tempfile.html#tempfile.NamedTemporaryFile
  try:
    temp_file = tempfile.NamedTemporaryFile(mode='w', delete=False)
    temp_file.writelines(json_to_jsonlines(entity_read_file.name))
    temp_file.close()
    # Random here used as a nonce for writing multiple files at once.
    filename = '%s_%s_%d.json' % (randint(1, 1000000), self.entity_type,
                                  time.time() * 1e+9)
    gcs_hook.upload(self.gcs_bucket, filename, temp_file.name)

  finally:
    if temp_file:
      temp_file.close()
    os.unlink(temp_file.name)

  return filename
