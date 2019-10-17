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


from orchestra.google.marketing_platform.utils.schema.erf import (
  ExchangeSettings,
  UniversalChannel,
  CostTrackingPixel,
  PartnerRevenueModel,
  CustomAffinity,
  UniversalSite,
  FrequencyCap,
  Creative,
  LineItem,
  Pixel,
  EntityCommonData,
  UserList,
  InventorySource,
  FreeFormTarget,
  DeviceCriteria,
  SupportedExchange,
  DataPartner,
  UserListPricing,
  TargetUnion,
  Target,
  Language,
  ApprovalStatus,
  InsertionOrder,
  Budget,
  UserListAdvertiserPricing,
  Browser,
  Advertiser,
  PartnerCosts,
  Campaign,
  GeoLocation,
  SelectionTarget,
  Isp,
  Partner,
  TargetList,
  AppCollection,
  FloodlightActivity,
  NegativeKeywordList
)


Entity_Schema_Lookup = {
    'ExchangeSettings': ExchangeSettings.ExchangeSettings_Schema,
    'UniversalChannel': UniversalChannel.UniversalChannel_Schema,
    'CostTrackingPixel': CostTrackingPixel.CostTrackingPixel_Schema,
    'PartnerRevenueModel': PartnerRevenueModel.PartnerRevenueModel_Schema,
    'CustomAffinity': CustomAffinity.CustomAffinity_Schema,
    'UniversalSite': UniversalSite.UniversalSite_Schema,
    'FrequencyCap': FrequencyCap.FrequencyCap_Schema,
    'Creative': Creative.Creative_Schema,
    'LineItem': LineItem.LineItem_Schema,
    'Pixel': Pixel.Pixel_Schema,
    'EntityCommonData': EntityCommonData.EntityCommonData_Schema,
    'UserList': UserList.UserList_Schema,
    'InventorySource': InventorySource.InventorySource_Schema,
    'FreeFormTarget': FreeFormTarget.FreeFormTarget_Schema,
    'DeviceCriteria': DeviceCriteria.DeviceCriteria_Schema,
    'SupportedExchange': SupportedExchange.SupportedExchange_Schema,
    'DataPartner': DataPartner.DataPartner_Schema,
    'UserListPricing': UserListPricing.UserListPricing_Schema,
    'TargetUnion': TargetUnion.TargetUnion_Schema,
    'Target': Target.Target_Schema,
    'Language': Language.Language_Schema,
    'ApprovalStatus': ApprovalStatus.ApprovalStatus_Schema,
    'InsertionOrder': InsertionOrder.InsertionOrder_Schema,
    'Budget': Budget.Budget_Schema,
    'UserListAdvertiserPricing': (
        UserListAdvertiserPricing.UserListAdvertiserPricing_Schema
    ),
    'Browser': Browser.Browser_Schema,
    'Advertiser': Advertiser.Advertiser_Schema,
    'PartnerCosts': PartnerCosts.PartnerCosts_Schema,
    'Campaign': Campaign.Campaign_Schema,
    'GeoLocation': GeoLocation.GeoLocation_Schema,
    'SelectionTarget': SelectionTarget.SelectionTarget_Schema,
    'Isp': Isp.Isp_Schema,
    'Partner': Partner.Partner_Schema,
    'TargetList': TargetList.TargetList_Schema,
    'AppCollection': AppCollection.AppCollection_Schema,
    'FloodlightActivity': FloodlightActivity.FloodlightActivity_Schema,
    'NegativeKeywordList': NegativeKeywordList.NegativeKeywordList_Schema,
}
