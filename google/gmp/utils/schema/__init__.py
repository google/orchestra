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


from google.gmp.utils.schema.ExchangeSettings import ExchangeSettings_Schema
from google.gmp.utils.schema.UniversalChannel import UniversalChannel_Schema
from google.gmp.utils.schema.CostTrackingPixel import CostTrackingPixel_Schema
from google.gmp.utils.schema.PartnerRevenueModel import PartnerRevenueModel_Schema
from google.gmp.utils.schema.CustomAffinity import CustomAffinity_Schema
from google.gmp.utils.schema.UniversalSite import UniversalSite_Schema
from google.gmp.utils.schema.FrequencyCap import FrequencyCap_Schema
from google.gmp.utils.schema.Creative import Creative_Schema
from google.gmp.utils.schema.LineItem import LineItem_Schema
from google.gmp.utils.schema.Pixel import Pixel_Schema
from google.gmp.utils.schema.EntityCommonData import EntityCommonData_Schema
from google.gmp.utils.schema.UserList import UserList_Schema
from google.gmp.utils.schema.InventorySource import InventorySource_Schema
from google.gmp.utils.schema.FreeFormTarget import FreeFormTarget_Schema
from google.gmp.utils.schema.DeviceCriteria import DeviceCriteria_Schema
from google.gmp.utils.schema.SupportedExchange import SupportedExchange_Schema
from google.gmp.utils.schema.DataPartner import DataPartner_Schema
from google.gmp.utils.schema.UserListPricing import UserListPricing_Schema
from google.gmp.utils.schema.TargetUnion import TargetUnion_Schema
from google.gmp.utils.schema.Target import Target_Schema
from google.gmp.utils.schema.Language import Language_Schema
from google.gmp.utils.schema.ApprovalStatus import ApprovalStatus_Schema
from google.gmp.utils.schema.InsertionOrder import InsertionOrder_Schema
from google.gmp.utils.schema.Budget import Budget_Schema
from google.gmp.utils.schema.UserListAdvertiserPricing import UserListAdvertiserPricing_Schema
from google.gmp.utils.schema.Browser import Browser_Schema
from google.gmp.utils.schema.Advertiser import Advertiser_Schema
from google.gmp.utils.schema.PartnerCosts import PartnerCosts_Schema
from google.gmp.utils.schema.Campaign import Campaign_Schema
from google.gmp.utils.schema.GeoLocation import GeoLocation_Schema
from google.gmp.utils.schema.SelectionTarget import SelectionTarget_Schema
from google.gmp.utils.schema.Isp import Isp_Schema
from google.gmp.utils.schema.Partner import Partner_Schema
from google.gmp.utils.schema.TargetList import TargetList_Schema
from google.gmp.utils.schema.AppCollection import AppCollection_Schema
from google.gmp.utils.schema.FloodlightActivity import FloodlightActivity_Schema
from google.gmp.utils.schema.NegativeKeywordList import NegativeKeywordList_Schema

Entity_Schema_Lookup = {
  'ExchangeSettings':ExchangeSettings_Schema,
  'UniversalChannel':UniversalChannel_Schema,
  'CostTrackingPixel':CostTrackingPixel_Schema,
  'PartnerRevenueModel':PartnerRevenueModel_Schema,
  'CustomAffinity':CustomAffinity_Schema,
  'UniversalSite':UniversalSite_Schema,
  'FrequencyCap':FrequencyCap_Schema,
  'Creative':Creative_Schema,
  'LineItem':LineItem_Schema,
  'Pixel':Pixel_Schema,
  'EntityCommonData':EntityCommonData_Schema,
  'UserList':UserList_Schema,
  'InventorySource':InventorySource_Schema,
  'FreeFormTarget':FreeFormTarget_Schema,
  'DeviceCriteria':DeviceCriteria_Schema,
  'SupportedExchange':SupportedExchange_Schema,
  'DataPartner':DataPartner_Schema,
  'UserListPricing':UserListPricing_Schema,
  'TargetUnion':TargetUnion_Schema,
  'Target':Target_Schema,
  'Language':Language_Schema,
  'ApprovalStatus':ApprovalStatus_Schema,
  'InsertionOrder':InsertionOrder_Schema,
  'Budget':Budget_Schema,
  'UserListAdvertiserPricing':UserListAdvertiserPricing_Schema,
  'Browser':Browser_Schema,
  'Advertiser':Advertiser_Schema,
  'PartnerCosts':PartnerCosts_Schema,
  'Campaign':Campaign_Schema,
  'GeoLocation':GeoLocation_Schema,
  'SelectionTarget':SelectionTarget_Schema,
  'Isp':Isp_Schema,
  'Partner':Partner_Schema,
  'TargetList':TargetList_Schema,
  'AppCollection':AppCollection_Schema,
  'FloodlightActivity':FloodlightActivity_Schema,
  'NegativeKeywordList':NegativeKeywordList_Schema,
}
