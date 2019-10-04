###########################################################################
#
#  Copyright 2017 Google Inc.
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

LineItem_Schema = [
   {
      "fields" : [
         {
            "type" : "INTEGER",
            "mode" : "NULLABLE",
            "name" : "id"
         },
         {
            "mode" : "NULLABLE",
            "type" : "STRING",
            "name" : "name"
         },
         {
            "mode" : "NULLABLE",
            "type" : "BOOLEAN",
            "name" : "active"
         },
         {
            "name" : "integration_code",
            "mode" : "NULLABLE",
            "type" : "STRING"
         }
      ],
      "mode" : "NULLABLE",
      "type" : "RECORD",
      "name" : "common_data"
   },
   {
      "mode" : "NULLABLE",
      "type" : "INTEGER",
      "name" : "line_item_type"
   },
   {
      "name" : "insertion_order_id",
      "type" : "INTEGER",
      "mode" : "NULLABLE"
   },
   {
      "mode" : "REPEATED",
      "type" : "INTEGER",
      "name" : "creative_ids"
   },
   {
      "mode" : "NULLABLE",
      "type" : "INTEGER",
      "name" : "max_cpm_advertiser_micros"
   },
   {
      "mode" : "NULLABLE",
      "type" : "INTEGER",
      "name" : "performance_goal"
   },
   {
      "name" : "goal_advertiser_micros",
      "type" : "INTEGER",
      "mode" : "NULLABLE"
   },
   {
      "mode" : "NULLABLE",
      "fields" : [
         {
            "name" : "type",
            "type" : "INTEGER",
            "mode" : "NULLABLE"
         },
         {
            "name" : "amount_advertiser_micros",
            "mode" : "NULLABLE",
            "type" : "INTEGER"
         },
         {
            "name" : "media_cost_markup_percent_millis",
            "mode" : "NULLABLE",
            "type" : "INTEGER"
         },
         {
            "name" : "post_view_conversion_tracking_fraction",
            "type" : "FLOAT",
            "mode" : "NULLABLE"
         }
      ],
      "type" : "RECORD",
      "name" : "partner_revenue_model"
   },
   {
      "fields" : [
         {
            "mode" : "NULLABLE",
            "type" : "INTEGER",
            "name" : "pixel_id"
         },
         {
            "name" : "view_window_minutes",
            "type" : "INTEGER",
            "mode" : "NULLABLE"
         },
         {
            "mode" : "NULLABLE",
            "type" : "INTEGER",
            "name" : "click_window_minutes"
         },
         {
            "mode" : "NULLABLE",
            "type" : "INTEGER",
            "name" : "floodlight_activity_id"
         },
      ],
      "mode" : "REPEATED",
      "type" : "RECORD",
      "name" : "cost_tracking_pixels"
   },
   {
      "name" : "budget",
      "mode" : "NULLABLE",
      "fields" : [
         {
            "name" : "start_time_usec",
            "mode" : "NULLABLE",
            "type" : "INTEGER"
         },
         {
            "name" : "end_time_usec",
            "type" : "INTEGER",
            "mode" : "NULLABLE"
         },
         {
            "name" : "max_impressions",
            "mode" : "NULLABLE",
            "type" : "INTEGER"
         },
         {
            "mode" : "NULLABLE",
            "type" : "INTEGER",
            "name" : "max_spend_advertiser_micros"
         },
         {
            "type" : "INTEGER",
            "mode" : "NULLABLE",
            "name" : "pacing_type"
         },
         {
            "mode" : "NULLABLE",
            "type" : "INTEGER",
            "name" : "pacing_max_impressions"
         },
         {
            "name" : "pacing_max_spend_advertiser_micros",
            "mode" : "NULLABLE",
            "type" : "INTEGER"
         },
         {
            "mode" : "NULLABLE",
            "type" : "INTEGER",
            "name" : "pacing_distribution"
         }
      ],
      "type" : "RECORD"
   },
   {
      "name" : "frequency_cap",
      "type" : "RECORD",
      "fields" : [
         {
            "name" : "max_impressions",
            "mode" : "NULLABLE",
            "type" : "INTEGER"
         },
         {
            "type" : "INTEGER",
            "mode" : "NULLABLE",
            "name" : "time_unit"
         },
         {
            "name" : "time_range",
            "type" : "INTEGER",
            "mode" : "NULLABLE"
         },
      ],
      "mode" : "NULLABLE"
   },
   {
      "type" : "RECORD",
      "mode" : "NULLABLE",
      "fields" : [
         {
            "name" : "inventory_sources",
            "fields" : [
               {
                  "name" : "criteria_id",
                  "type" : "INTEGER",
                  "mode" : "NULLABLE"
               },
                     {
                        "name" : "parameter",
                        "type" : "STRING",
                        "mode" : "NULLABLE"
                     },
               {
                  "name" : "excluded",
                  "type" : "BOOLEAN",
                  "mode" : "NULLABLE"
               }
            ],
            "mode" : "REPEATED",
            "type" : "RECORD"
         },
         {
            "name" : "geo_locations",
            "type" : "RECORD",
            "mode" : "REPEATED",
            "fields" : [
               {
                  "name" : "criteria_id",
                  "type" : "INTEGER",
                  "mode" : "NULLABLE"
               },
                     {
                        "name" : "parameter",
                        "type" : "STRING",
                        "mode" : "NULLABLE"
                     },
               {
                  "name" : "excluded",
                  "type" : "BOOLEAN",
                  "mode" : "NULLABLE"
               }
            ]
         },
         {
            "name" : "ad_position",
            "fields" : [
               {
                  "name" : "criteria_id",
                  "mode" : "NULLABLE",
                  "type" : "INTEGER"
               },
                     {
                        "name" : "parameter",
                        "type" : "STRING",
                        "mode" : "NULLABLE"
                     },
               {
                  "type" : "BOOLEAN",
                  "mode" : "NULLABLE",
                  "name" : "excluded"
               }
            ],
            "mode" : "NULLABLE",
            "type" : "RECORD"
         },
         {
            "type" : "RECORD",
            "fields" : [
               {
                  "name" : "criteria_id",
                  "mode" : "NULLABLE",
                  "type" : "INTEGER"
               },
               {
                  "name" : "excluded",
                  "mode" : "NULLABLE",
                  "type" : "BOOLEAN"
               }
            ],
            "mode" : "NULLABLE",
            "name" : "net_speed"
         },
         {
            "mode" : "NULLABLE",
            "fields" : [
               {
                  "name" : "union",
                  "type" : "RECORD",
                  "mode" : "REPEATED",
                  "fields" : [
                     {
                        "type" : "INTEGER",
                        "mode" : "NULLABLE",
                        "name" : "criteria_id"
                     },
                     {
                        "name" : "parameter",
                        "type" : "STRING",
                        "mode" : "NULLABLE"
                     }
                  ]
               },
               {
                  "name" : "excluded",
                  "type" : "BOOLEAN",
                  "mode" : "NULLABLE"
               }
            ],
            "type" : "RECORD",
            "name" : "browsers"
         },
         {
            "type" : "RECORD",
            "mode" : "REPEATED",
            "fields" : [
               {
                  "name" : "criteria_id",
                  "mode" : "NULLABLE",
                  "type" : "INTEGER"
               },
                     {
                        "name" : "parameter",
                        "type" : "STRING",
                        "mode" : "NULLABLE"
                     },
               {
                  "mode" : "NULLABLE",
                  "type" : "BOOLEAN",
                  "name" : "excluded"
               }
            ],
            "name" : "device_criteria"
         },
         {
            "type" : "RECORD",
            "mode" : "NULLABLE",
            "fields" : [
               {
                  "name" : "union",
                  "mode" : "REPEATED",
                  "fields" : [
                     {
                        "type" : "INTEGER",
                        "mode" : "NULLABLE",
                        "name" : "criteria_id"
                     },
                     {
                        "name" : "parameter",
                        "type" : "STRING",
                        "mode" : "NULLABLE"
                     },
               {
                  "mode" : "NULLABLE",
                  "type" : "BOOLEAN",
                  "name" : "excluded"
               }
                  ],
                  "type" : "RECORD"
               },
               {
                  "mode" : "NULLABLE",
                  "type" : "BOOLEAN",
                  "name" : "excluded"
               }
            ],
            "name" : "languages"
         },
         {
            "fields" : [
               {
                  "mode" : "REPEATED",
                  "fields" : [
                     {
                        "mode" : "NULLABLE",
                        "type" : "INTEGER",
                        "name" : "criteria_id"
                     },
                     {
                        "name" : "parameter",
                        "type" : "STRING",
                        "mode" : "NULLABLE"
                     },
                     {
                        "type" : "STRING",
                        "mode" : "NULLABLE",
                        "name" : "excluded"
                     }
                  ],
                  "type" : "RECORD",
                  "name" : "union"
               },
               {
                  "name" : "excluded",
                  "mode" : "NULLABLE",
                  "type" : "BOOLEAN"
               }
            ],
            "mode" : "NULLABLE",
            "type" : "RECORD",
            "name" : "day_parting"
         },
         {
            "fields" : [
               {
                  "name" : "union",
                  "mode" : "REPEATED",
                  "fields" : [
                     {
                        "name" : "criteria_id",
                        "mode" : "NULLABLE",
                        "type" : "INTEGER"
                     },
                     {
                        "name" : "parameter",
                        "type" : "STRING",
                        "mode" : "NULLABLE"
                     },
                     {
                        "name" : "excluded",
                        "type" : "STRING",
                        "mode" : "NULLABLE"
                     }
                  ],
                  "type" : "RECORD"
               },
               {
                  "mode" : "NULLABLE",
                  "type" : "BOOLEAN",
                  "name" : "excluded"
               }
            ],
            "mode" : "REPEATED",
            "type" : "RECORD",
            "name" : "audience_intersect"
         },
         {
            "fields" : [
               {
                  "name" : "union",
                  "mode" : "REPEATED",
                  "fields" : [
                     {
                        "name" : "criteria_id",
                        "mode" : "NULLABLE",
                        "type" : "INTEGER"
                     },
                     {
                        "name" : "parameter",
                        "type" : "STRING",
                        "mode" : "NULLABLE"
                     },
                     {
                        "name" : "excluded",
                        "type" : "STRING",
                        "mode" : "NULLABLE"
                     }
                  ],
                  "type" : "RECORD"
               },
               {
                  "mode" : "NULLABLE",
                  "type" : "BOOLEAN",
                  "name" : "excluded"
               }
            ],
            "mode" : "REPEATED",
            "type" : "RECORD",
            "name" : "isps"
         },
         {
            "mode" : "REPEATED",
            "fields" : [
               {
                  "name" : "criteria_id",
                  "type" : "INTEGER",
                  "mode" : "NULLABLE"
               },
               {
                  "name" : "parameter",
                  "type" : "STRING",
                  "mode" : "NULLABLE"
               },
               {
                  "mode" : "NULLABLE",
                  "type" : "BOOLEAN",
                  "name" : "excluded"
               }
            ],
            "type" : "RECORD",
            "name" : "keywords"
         },
         {
            "name" : "kct_include_uncrawled_sites",
            "type" : "BOOLEAN",
            "mode" : "NULLABLE"
         },
         {
            "name" : "page_categories",
            "fields" : [
               {
                  "name" : "criteria_id",
                  "mode" : "NULLABLE",
                  "type" : "INTEGER"
               },
                     {
                        "name" : "parameter",
                        "type" : "STRING",
                        "mode" : "NULLABLE"
                     },
               {
                  "name" : "excluded",
                  "type" : "BOOLEAN",
                  "mode" : "NULLABLE"
               }
            ],
            "mode" : "REPEATED",
            "type" : "RECORD"
         },
         {
            "type" : "RECORD",
            "mode" : "REPEATED",
            "fields" : [
               {
                  "name" : "criteria_id",
                  "type" : "INTEGER",
                  "mode" : "NULLABLE"
               },
                     {
                        "name" : "parameter",
                        "type" : "STRING",
                        "mode" : "NULLABLE"
                     },
               {
                  "name" : "excluded",
                  "type" : "BOOLEAN",
                  "mode" : "NULLABLE"
               }
            ],
            "name" : "universal_channels"
         },
         {
            "name" : "sites",
            "fields" : [
               {
                  "name" : "criteria_id",
                  "type" : "INTEGER",
                  "mode" : "NULLABLE"
               },
               {
                  "name" : "parameter",
                  "type" : "STRING",
                  "mode" : "NULLABLE"
               },
               {
                  "type" : "BOOLEAN",
                  "mode" : "NULLABLE",
                  "name" : "excluded"
               }
            ],
            "mode" : "REPEATED",
            "type" : "RECORD"
         },
         {
            "name" : "brand_safety",
            "type" : "RECORD",
            "mode" : "NULLABLE",
            "fields" : [
               {
                  "mode" : "NULLABLE",
                  "type" : "INTEGER",
                  "name" : "criteria_id"
               },
                     {
                        "name" : "parameter",
                        "type" : "STRING",
                        "mode" : "NULLABLE"
                     },
               {
                  "mode" : "NULLABLE",
                  "type" : "BOOLEAN",
                  "name" : "excluded"
               }
            ]
         }
      ],
      "name" : "target_list"
   },
   {
      "name" : "partner_costs",
      "mode" : "NULLABLE",
      "fields" : [
         {
            "mode" : "NULLABLE",
            "type" : "INTEGER",
            "name" : "cpm_fee_1_advertiser_micros"
         },
         {
            "name" : "cpm_fee_2_advertiser_micros",
            "type" : "INTEGER",
            "mode" : "NULLABLE"
         },
         {
            "mode" : "NULLABLE",
            "type" : "INTEGER",
            "name" : "cpm_fee_3_advertiser_micros"
         },
         {
            "name" : "cpm_fee_4_advertiser_micros",
            "type" : "INTEGER",
            "mode" : "NULLABLE"
         },
         {
            "mode" : "NULLABLE",
            "type" : "INTEGER",
            "name" : "cpm_fee_5_advertiser_micros"
         },
         {
            "name" : "media_fee_percent_1_millis",
            "type" : "INTEGER",
            "mode" : "NULLABLE"
         },
         {
            "name" : "media_fee_percent_2_millis",
            "mode" : "NULLABLE",
            "type" : "INTEGER"
         },
         {
            "mode" : "NULLABLE",
            "type" : "INTEGER",
            "name" : "media_fee_percent_3_millis"
         },
         {
            "name" : "media_fee_percent_4_millis",
            "mode" : "NULLABLE",
            "type" : "INTEGER"
         },
         {
            "mode" : "NULLABLE",
            "type" : "INTEGER",
            "name" : "media_fee_percent_5_millis"
         },
         {
            "name" : "cpm_fee_1_cost_type",
            "type" : "INTEGER",
            "mode" : "NULLABLE"
         },
         {
            "name" : "cpm_fee_2_cost_type",
            "type" : "INTEGER",
            "mode" : "NULLABLE"
         },
         {
            "type" : "INTEGER",
            "mode" : "NULLABLE",
            "name" : "cpm_fee_3_cost_type"
         },
         {
            "mode" : "NULLABLE",
            "type" : "INTEGER",
            "name" : "cpm_fee_4_cost_type"
         },
         {
            "mode" : "NULLABLE",
            "type" : "INTEGER",
            "name" : "cpm_fee_5_cost_type"
         },
         {
            "type" : "INTEGER",
            "mode" : "NULLABLE",
            "name" : "media_fee_percent_1_cost_type"
         },
         {
            "name" : "media_fee_percent_2_cost_type",
            "mode" : "NULLABLE",
            "type" : "INTEGER"
         },
         {
            "name" : "media_fee_percent_3_cost_type",
            "type" : "INTEGER",
            "mode" : "NULLABLE"
         },
         {
            "type" : "INTEGER",
            "mode" : "NULLABLE",
            "name" : "media_fee_percent_4_cost_type"
         },
         {
            "type" : "INTEGER",
            "mode" : "NULLABLE",
            "name" : "media_fee_percent_5_cost_type"
         },
         {
            "name" : "cpm_fee_1_bill_to_type",
            "type" : "INTEGER",
            "mode" : "NULLABLE"
         },
         {
            "mode" : "NULLABLE",
            "type" : "INTEGER",
            "name" : "cpm_fee_2_bill_to_type"
         },
         {
            "name" : "cpm_fee_3_bill_to_type",
            "mode" : "NULLABLE",
            "type" : "INTEGER"
         },
         {
            "name" : "cpm_fee_4_bill_to_type",
            "type" : "INTEGER",
            "mode" : "NULLABLE"
         },
         {
            "name" : "cpm_fee_5_bill_to_type",
            "type" : "INTEGER",
            "mode" : "NULLABLE"
         },
         {
            "mode" : "NULLABLE",
            "type" : "INTEGER",
            "name" : "media_fee_percent_1_bill_to_type"
         },
         {
            "type" : "INTEGER",
            "mode" : "NULLABLE",
            "name" : "media_fee_percent_2_bill_to_type"
         },
         {
            "type" : "INTEGER",
            "mode" : "NULLABLE",
            "name" : "media_fee_percent_3_bill_to_type"
         },
         {
            "mode" : "NULLABLE",
            "type" : "INTEGER",
            "name" : "media_fee_percent_4_bill_to_type"
         },
         {
            "mode" : "NULLABLE",
            "type" : "INTEGER",
            "name" : "media_fee_percent_5_bill_to_type"
         }
      ],
      "type" : "RECORD"
   }
]
