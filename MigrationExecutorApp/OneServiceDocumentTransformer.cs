using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json.Serialization;
using MsnAction = Msn.TagsDataModel.TagEntityLib.Action;
using MsnUser = Msn.TagsDataModel.TagEntityLib.User;
using Msn.TagsDataModel.TagEntityLib;
using System.Security.Cryptography;
using Azure.Storage.Blobs;
using System.IO;

namespace MigrationConsoleApp
{
    public class OneServiceDocumentTransformer
    {
        public static JsonSerializerSettings jsonSettings = new JsonSerializerSettings()
        {
            ContractResolver = new CamelCasePropertyNamesContractResolver(),
            NullValueHandling = NullValueHandling.Ignore
        };

        public static IEnumerable<Document> TransformDocument(Document sourceDoc, BlobContainerClient containerClient)
        {
            var docs = new List<Document>();

            JObject obj = (JObject)JToken.FromObject(sourceDoc);
            var type = (string)obj["itemKey"];
            var userId = "U_a-" + (string)obj["userId"];

            if (string.IsNullOrWhiteSpace(type))
            {
                return new List<Document>().AsEnumerable();
            }

            try
            {
                switch (type)
                {
                    case "Interests.NewsQuery":
                        return GetNewsAction(obj, userId);
                    case "Interests.Weather":
                        return GetWeatherAction(obj, userId, containerClient);
                    case "Interests.TrendingOnBingTopic":
                        return GetTrendingOnBingAction(obj, userId, containerClient);
                    case "InterestSettings":
                        return GetInterestSettings(obj, userId);
                    /*case "Interests.SportsTeam":
                        return GetSportsAction(obj, userId);
                    case "Interests.FinanceSecurity":
                        return GetFinanceAction(obj, userId);*/
                    default:
                        return new List<Document>().AsEnumerable();
                }
            }
            catch (Exception e)
            {
                WriteExceptionToBlob(containerClient, e, sourceDoc.ToString(), userId);
                return new List<Document>().AsEnumerable();
            }
        }

        public static IEnumerable<Document> GetActions(JObject docObj, string userId)
        {
            var docs = new List<Document>();
            var data = docObj["data"]?.ToObject<Dictionary<string, object>>();
            var id = docObj["id"].ToString();

            MsnAction a = new MsnAction
            {
                TargetId = id,
                TargetType = "Team",
                ActionType = "FollowTest",
                Id = "f_" + userId + "_" + id,
                OwnerId = userId
            };

            if (data != null && data.Any())
            {
                a.Metadata = data;
            }

            var dynamicDoc = JsonConvert.DeserializeObject<dynamic>(JsonConvert.SerializeObject(a, jsonSettings));

            using (JsonReader reader = new JTokenReader(dynamicDoc))
            {
                Document d = new Document();
                d.LoadFrom(reader);
                docs.Add(d);
            }

            return docs.AsEnumerable();
        }

        public static IEnumerable<Document> GetNewsAction(JObject docObj, string userId)
        {
            // Preconditions //
            var docs = new List<Document>();
            var data = docObj["data"].ToObject<Dictionary<string, object>>();
            if (data == null || !data.Any())
            {
                return docs.AsEnumerable();
            }

            var tmpData = data.FirstOrDefault();
            if (!tmpData.Key.Equals("data", StringComparison.OrdinalIgnoreCase))
            {
                return docs.AsEnumerable();
            }
            // End Preconditions //

            int rank = 0;
            JArray dataArr = JArray.Parse(tmpData.Value.ToString());
            foreach (JObject item in dataArr)
            {
                var displayName = item["DisplayName"]?.ToString();
                JObject interestTypeSpecificFields = (JObject)item["InterestTypeSpecificFields"];
                var categoryId = interestTypeSpecificFields?["PdpCategoryId"]?.ToString();
                var newsIdentifier = interestTypeSpecificFields?["NewsIdentifier"]?.ToString();

                JObject metadata = new JObject();
                if (!string.IsNullOrWhiteSpace(displayName))
                {
                    metadata.Add("displayName", displayName);
                }

                if (!string.IsNullOrWhiteSpace(categoryId))
                {
                    metadata.Add("pdpCategoryId", categoryId);
                }

                if (!string.IsNullOrWhiteSpace(newsIdentifier))
                {
                    metadata.Add("newsIdentifier", newsIdentifier);
                }

                MsnAction a = new MsnAction
                {
                    DefinitionName = $"{displayName}.{categoryId}.{newsIdentifier}",
                    TargetType = nameof(UserQuery),
                    ActionType = "Follow",
                    Rank = rank++,
                    Metadata = metadata,
                    OwnerId = userId,
                    PartitionKey = userId
                };

                if (long.TryParse(item["CreatedTimeInTicksUtc"]?.ToString(), out long createdTimeInTicks))
                {
                    a.CreatedDateTime = new DateTime(createdTimeInTicks).ToString(TagEntityUtil.TimeFormat);
                }

                a.TargetId = ToHashGuid(a.DefinitionName);
                a.Id = TagEntityUtil.GetId("Follow", userId, a.TargetId);
                var dynamicDoc = JsonConvert.DeserializeObject<dynamic>(JsonConvert.SerializeObject(a, jsonSettings));

                using (JsonReader reader = new JTokenReader(dynamicDoc))
                {
                    Document d = new Document();
                    d.LoadFrom(reader);
                    docs.Add(d);
                }
            }

            return docs.AsEnumerable();
        }



        /*  Weather Data Schema  
         *  "[{
                \"CorrelationGuid\":\"WeatherForecastNearby\",
                \"DomainSpecificId\":null,
                \"DisplayName\":\"Local weather\",
                \"CreatedTimeInTicksUtc\":636518200278340094,
                \"InterestType\":12,
                \"InterestTypeSpecificFields\":
                {
                    \"Latitude\":\"0.00\",
                    \"Longitude\":\"0.00\",
                    \"City\":\"\",
                    \"State\":\"\",
                    \"CountryRegion\":\"\",
                    \"ISOCode\":\"\",
                    \"LocationType\":null,
                    \"EntityType\":\"\",
                    \"IsWeatherNearby\":\"true\"
                }}]"

            "[{
                \"CorrelationGuid\":\"7140f49c-17f2-4cc3-987a-27f99c628d65\",
                \"DomainSpecificId\":null,
                \"DisplayName\":\"New York, New York\",
                \"CreatedTimeInTicksUtc\":637232429094084768,
                \"InterestType\":12,
                \"InterestTypeSpecificFields\":
                {
                    \"Latitude\":\"40.71\",
                    \"Longitude\":\"-74.01\",
                    \"City\":\"New York\",
                    \"State\":\"New York\",
                    \"CountryRegion\":\"United States\",
                    \"ISOCode\":\"US\",
                    \"LocationType\":null,
                    \"EntityType\":\"\",
                    \"IsWeatherNearby\":null
                }
            },{
                \"CorrelationGuid\":\"c1c8b390-1514-497c-9f71-ea4413f85fed\",
                \"DomainSpecificId\":null,
                \"DisplayName\":\"Pittsburgh, Pennsylvania\",
                \"CreatedTimeInTicksUtc\":637232429287479152,
                \"InterestType\":12,
                \"InterestTypeSpecificFields\":
                {
                    \"Latitude\":\"40.44\",
                    \"Longitude\":\"-80.00\",
                    \"City\":\"Pittsburgh\",
                    \"State\":\"Pennsylvania\",
                    \"CountryRegion\":\"United States\",
                    \"ISOCode\":\"US\",
                    \"LocationType\":null,
                    \"EntityType\":\"\",
                    \"IsWeatherNearby\":null
                }
            },{
                \"CorrelationGuid\":\"b1f47435-aec5-4007-9d46-65b4ed4215c5\",
                \"DomainSpecificId\":null,
                \"DisplayName\":\"Washington, District of Columbia\",
                \"CreatedTimeInTicksUtc\":637232429538753923,
                \"InterestType\":12,
                \"InterestTypeSpecificFields\":{                    \"Latitude\":\"38.89\",		            \"Longitude\":\"-77.03\",		            \"City\":\"Washington\",		            \"State\":\"District of Columbia\",		            \"CountryRegion\":\"United States\",		            \"ISOCode\":\"US\",		            \"LocationType\":null,		            \"EntityType\":\"\",		            \"IsWeatherNearby\":null
                }
            }]"


        */
        public static IEnumerable<Document> GetWeatherAction(JObject docObj, string userId, BlobContainerClient containerClient)
        {
            // Preconditions //
            var docs = new List<Document>();
            var data = docObj["data"].ToObject<Dictionary<string, object>>();
            if (data == null || !data.Any())
            {
                return docs.AsEnumerable();
            }

            var tmpData = data.FirstOrDefault();
            if (!tmpData.Key.Equals("data", StringComparison.OrdinalIgnoreCase))
            {
                return docs.AsEnumerable();
            }
            // End Preconditions //

            JArray dataArr = JArray.Parse(tmpData.Value.ToString());
            int rank = 0;
            foreach (JObject item in dataArr)
            {
                JObject interestTypeSpecificFields = (JObject)item["InterestTypeSpecificFields"];
                var latitude = interestTypeSpecificFields?["Latitude"]?.ToString();
                var longitude = interestTypeSpecificFields?["Longitude"]?.ToString();

                if (string.IsNullOrWhiteSpace(latitude) || string.IsNullOrWhiteSpace(longitude))
                {
                    Trace.TraceInformation("Skipping Location for user {0} due to missing Latitude and Longitude, doc id {1}", userId, (string)docObj["id"]);
                    WriteFailedTransformationToBlob(containerClient, docObj.ToString(), userId, "WeatherNoLatOrLon");
                    continue;
                }

                JObject metadata = new JObject();
                var displayName = item["DisplayName"]?.ToString();
                var correlationGuid = item["CorrelationGuid"]?.ToString();

                if (!string.IsNullOrWhiteSpace(displayName))
                {
                    metadata.Add("displayName", displayName);
                }

                foreach (JProperty property in interestTypeSpecificFields.Properties())
                {
                    var value = property.Value?.ToString();
                    if (!string.IsNullOrWhiteSpace(value))
                    {
                        metadata.Add(ToLowerFirstChar(property.Name), value);
                    }
                }

                MsnAction a = new MsnAction
                {
                    DefinitionName = $"{latitude},{longitude}",
                    TargetType = nameof(StructureDataTypes.Location),
                    ActionType = "Follow",
                    Degree = "FavoriteLocation",
                    Metadata = metadata,
                    OwnerId = userId,
                    Rank = rank++,
                    PartitionKey = userId
                };

                if (correlationGuid == "WeatherForecastNearby")
                {
                    a.Degree = "WeatherForecastNearby";
                }

                if (long.TryParse(item["CreatedTimeInTicksUtc"]?.ToString(), out long createdTimeInTicks))
                {
                    a.CreatedDateTime = new DateTime(createdTimeInTicks).ToString(TagEntityUtil.TimeFormat);
                }

                a.TargetId = ToHashGuid(a.DefinitionName);
                a.Id = TagEntityUtil.GetId("Follow", userId, a.TargetId);
                var dynamicDoc = JsonConvert.DeserializeObject<dynamic>(JsonConvert.SerializeObject(a, jsonSettings));

                using (JsonReader reader = new JTokenReader(dynamicDoc))
                {
                    Document d = new Document();
                    d.LoadFrom(reader);
                    docs.Add(d);
                }
            }

            return docs.AsEnumerable();
        }

        public static IEnumerable<Document> GetTrendingOnBingAction(JObject docObj, string userId, BlobContainerClient containerClient)
        {
            // Preconditions //
            var docs = new List<Document>();
            var data = docObj["data"].ToObject<Dictionary<string, object>>();
            if (data == null || !data.Any())
            {
                return docs.AsEnumerable();
            }

            var tmpData = data.FirstOrDefault();
            if (!tmpData.Key.Equals("data", StringComparison.OrdinalIgnoreCase))
            {
                return docs.AsEnumerable();
            }
            // End Preconditions //

            JArray dataArr = JArray.Parse(tmpData.Value.ToString());
            foreach (JObject item in dataArr)
            {
                JObject metadata = new JObject();
                var displayName = item["DisplayName"]?.ToString();

                if (!string.IsNullOrWhiteSpace(displayName))
                {
                    metadata.Add("displayName", displayName);
                }

                JObject interestTypeSpecificFields = (JObject)item["InterestTypeSpecificFields"];
                var newsCategory = interestTypeSpecificFields?["NewsCategory"]?.ToString();
                var preference = interestTypeSpecificFields?["PreferenceValue"]?.ToString();

                if (string.IsNullOrWhiteSpace(newsCategory))
                {
                    Trace.TraceInformation("Skipping TrendingOnbing for user {0} due to missing NewsCategory, doc id {1}", userId, (string)docObj["id"]);
                    WriteFailedTransformationToBlob(containerClient, docObj.ToString(), userId, "TrendingOnBingNoNewsCategory");
                    return new List<Document>();
                }

                metadata.Add("newsCategory", newsCategory);

                if (!string.IsNullOrWhiteSpace(preference))
                {
                    metadata.Add("preferenceValue", preference);
                }

                MsnAction a = new MsnAction
                {
                    ActionType = "Preference",
                    TargetType = "TrendingOnBing",
                    Metadata = metadata,
                    OwnerId = userId,
                    PartitionKey = userId
                };

                if (int.TryParse(newsCategory, out int intValue))
                {
                    a.DefinitionName = ((NewsCategory)intValue).ToString();
                }
                else
                {
                    a.DefinitionName = newsCategory;
                }

                if (long.TryParse(item["CreatedTimeInTicksUtc"]?.ToString(), out long createdTimeInTicks))
                {
                    a.CreatedDateTime = new DateTime(createdTimeInTicks).ToString(TagEntityUtil.TimeFormat);
                }

                a.TargetId = ToHashGuid(a.DefinitionName);
                a.Id = $"pr_{userId}_{a.TargetId}";
                //TagEntityUtil.GetId("Follow", userId, a.TargetId);
                var dynamicDoc = JsonConvert.DeserializeObject<dynamic>(JsonConvert.SerializeObject(a, jsonSettings));

                using (JsonReader reader = new JTokenReader(dynamicDoc))
                {
                    Document d = new Document();
                    d.LoadFrom(reader);
                    docs.Add(d);
                }
            }

            return docs.AsEnumerable();
        }

        public static IEnumerable<Document> GetInterestSettings(JObject docObj, string userId)
        {
            // Preconditions //
            var docs = new List<Document>();
            var data = docObj["data"].ToObject<Dictionary<string, object>>();
            if (data == null || !data.Any())
            {
                return docs.AsEnumerable();
            }

            var tmpData = data.FirstOrDefault();
            if (!tmpData.Key.Equals("data", StringComparison.OrdinalIgnoreCase))
            {
                return docs.AsEnumerable();
            }
            // End Preconditions //

            JObject dataObj = JObject.Parse(tmpData.Value.ToString());
            var isPersonalizationEnabled = dataObj?["IsPersonalizationEnabled"]?.ToString();
            var lastChangeTime = dataObj?["LastChangeTimeInTicksUtcOfIsPersonalizationEnabled"]?.ToString();

            JToken interestTypeSettings = dataObj?["InterestTypeSettings"];
            var temperatureUnit = string.Empty;

            if (interestTypeSettings != null && interestTypeSettings.Type != JTokenType.Null)
            {
                temperatureUnit = interestTypeSettings?["Weather"]?["AdditionalSettings"]?["ShouldUseCelsius"]?.ToString();
            }

            MsnUser user = new MsnUser { PartitionKey = userId, Id = userId };
            if (!string.IsNullOrWhiteSpace(isPersonalizationEnabled))
            {
                user.UserSettings.Add("isPersonalizationEnabled", isPersonalizationEnabled);
            }

            if (!string.IsNullOrWhiteSpace(lastChangeTime))
            {
                user.UserSettings.Add("lastChangeTimeInTicksUtcOfIsPersonalizationEnabled", lastChangeTime);
            }

            if (!string.IsNullOrWhiteSpace(temperatureUnit))
            {
                user.UserSettings.Add("shouldUseCelsius", temperatureUnit);
            }

            var dynamicDoc = JsonConvert.DeserializeObject<dynamic>(JsonConvert.SerializeObject(user, jsonSettings));

            using (JsonReader reader = new JTokenReader(dynamicDoc))
            {
                Document d = new Document();
                d.LoadFrom(reader);
                docs.Add(d);
            }

            return docs.AsEnumerable();
        }

        #region Sports and Finance
        public static IEnumerable<Document> GetSportsAction(JObject docObj, string userId)
        {
            // Preconditions //
            var docs = new List<Document>();
            var data = docObj["data"].ToObject<Dictionary<string, object>>();
            if (data == null || !data.Any())
            {
                return docs.AsEnumerable();
            }

            var tmpData = data.FirstOrDefault();
            if (!tmpData.Key.Equals("data", StringComparison.OrdinalIgnoreCase))
            {
                return docs.AsEnumerable();
            }
            // End Preconditions //

            JArray dataArr = JArray.Parse(tmpData.Value.ToString());
            int rank = 1;

            foreach (JObject item in dataArr)
            {
                JObject interestTypeSpecificFields = (JObject)item["InterestTypeSpecificFields"];

                var teamId = interestTypeSpecificFields.GetValue("MsnShortTeamId")?.ToString();

                if (string.IsNullOrWhiteSpace(teamId))
                {
                    Trace.TraceInformation("Skipping doc id - {0}, user {1}", (string)docObj["id"], userId);
                    continue;
                }

                MsnAction a = new MsnAction
                {
                    TargetId = teamId,
                    TargetType = "Team",
                    ActionType = "Follow",
                    Id = "f_" + userId + "_" + teamId,
                    Metadata = item,
                    Rank = rank++,
                    OwnerId = userId
                };

                var dynamicDoc = JsonConvert.DeserializeObject<dynamic>(JsonConvert.SerializeObject(a, jsonSettings));

                using (JsonReader reader = new JTokenReader(dynamicDoc))
                {
                    Document d = new Document();
                    d.LoadFrom(reader);
                    docs.Add(d);
                }
            }

            return docs.AsEnumerable();
        }

        public static IEnumerable<Document> GetFinanceAction(JObject docObj, string userId)
        {
            // Preconditions //
            var docs = new List<Document>();
            var data = docObj["data"].ToObject<Dictionary<string, object>>();
            if (data == null || !data.Any())
            {
                return docs.AsEnumerable();
            }

            var tmpData = data.FirstOrDefault();
            if (!tmpData.Key.Equals("data", StringComparison.OrdinalIgnoreCase))
            {
                return docs.AsEnumerable();
            }
            // End Preconditions //

            JArray dataArr = JArray.Parse(tmpData.Value.ToString());
            int rank = 1;

            foreach (JObject item in dataArr)
            {
                JObject interestTypeSpecificFields = (JObject)item["InterestTypeSpecificFields"];

                var id = interestTypeSpecificFields.GetValue("MorningStarId")?.ToString();

                if (string.IsNullOrWhiteSpace(id))
                {
                    Trace.TraceInformation("Skipping doc id - {0}, user {1}", (string)docObj["id"], userId);
                    continue;
                }

                MsnAction a = new MsnAction
                {
                    TargetId = id,
                    TargetType = "Finance",
                    ActionType = "Follow",
                    Id = "f_" + userId + "_" + id,
                    Metadata = item,
                    Rank = rank++,
                    OwnerId = userId
                };

                var dynamicDoc = JsonConvert.DeserializeObject<dynamic>(JsonConvert.SerializeObject(a, jsonSettings));

                using (JsonReader reader = new JTokenReader(dynamicDoc))
                {
                    Document d = new Document();
                    d.LoadFrom(reader);
                    docs.Add(d);
                }
            }

            return docs.AsEnumerable();
        }

        #endregion Sports and Finance
        public static string ToLowerFirstChar(string input)
        {
            var words = input.Split('.');
            for (var i = 0; i < words.Length; i++)
            {
                if (!string.IsNullOrEmpty(words[i]) && char.IsUpper(words[i][0]))
                {
                    words[i] = char.ToLowerInvariant(words[i][0]) + words[i].Substring(1);
                }
            }

            return string.Join(".", words);
        }

        public static string ToHashGuid(string value)
        {
            using (MD5 md5 = MD5.Create())
            {
                byte[] hash = md5.ComputeHash(Encoding.UTF8.GetBytes(value));
                var result = new Guid(hash);
                return result.ToString();
            }
        }

        enum NewsCategory
        {
            World = 0,
            US = 1,
            Business = 2,
            Entertainment = 3,
            ScienceAndTechnology = 4,
            Sports = 5,
            Politics = 6,
            Lifestyle = 7,
            Health = 8
        }

        /// <summary>
        /// Records an Exception to Blob Store
        /// </summary>
        private static void WriteExceptionToBlob(BlobContainerClient containerClient, Exception e, string failedDocument, string failedUserId)
        {
            if (containerClient == null)
            {
                return;
            }

            BlobClient blobClient = containerClient.GetBlobClient($"Exception_{e.HResult}_{failedUserId}_{Guid.NewGuid()}.csv");
            byte[] byteArray = Encoding.ASCII.GetBytes(e.Message.ToString() + failedDocument);

            using (var ms = new MemoryStream(byteArray))
            {
                blobClient.UploadAsync(ms, overwrite: true);
            }
        }

        /// <summary>
        /// Records a transformation failure to Blob Storage
        /// </summary>
        private static void WriteFailedTransformationToBlob(BlobContainerClient containerClient, string failedDocument, string failedUserId, string failureType)
        {
            if (containerClient == null)
            {
                return;
            }

            BlobClient blobClient = containerClient.GetBlobClient($"TransformationFailure_{failureType}_{failedUserId}_{Guid.NewGuid()}.csv");
            byte[] byteArray = Encoding.ASCII.GetBytes(failedDocument);

            using (var ms = new MemoryStream(byteArray))
            {
                blobClient.UploadAsync(ms, overwrite: true);
            }
        }
    }
}
