﻿using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using Dapper.Contrib.Extensions;
using NLog;

namespace Kartverket.Geosynkronisering.Subscriber.DL
{
    public static class SubscriberDatasetManager
    {
        private static readonly Logger logger = LogManager.GetCurrentClassLogger(); // NLog for logging (nuget package)

        public static SubscriberDataset GetDataset(string datasetName)
        {
            using (var localDb = new geosyncDBEntities())
            {
                Dataset dataset = (from d in localDb.Dataset where d.Name == datasetName select d).FirstOrDefault();
                if (dataset == null)
                    return null;

                var geoClientDataset = MapToGeoClientDataset(dataset);

                return geoClientDataset;
            }
        }

        public static SubscriberDataset GetDataset(int datasetId)
        {
            using (var localDb = new geosyncDBEntities())
            {
                Dataset dataset = (from d in localDb.Dataset where d.DatasetId == datasetId select d).FirstOrDefault();
                if (dataset == null)
                    return null;

                var geoClientDataset = MapToGeoClientDataset(dataset);
                return geoClientDataset;
            }
        }

        public static List<SubscriberDataset> GetAllDataset()
        {
            using (var localDb = new geosyncDBEntities())
            {
                List<Dataset> datasets = (from d in localDb.Dataset select d).ToList();

                var geoClientDatasets = new List<SubscriberDataset>();

                foreach (var dataset in datasets)
                {
                    geoClientDatasets.Add(MapToGeoClientDataset(dataset));
                }
                return geoClientDatasets;
            }
        }

        public static string GetDatasource()
        {
            return geosyncDBEntities.Connection.DataSource;
        }



        public static bool UpdateDataset(SubscriberDataset geoClientDataset)
        {
            using (var localDb = new geosyncDBEntities())
            {
                Dataset dataset =
                    (from d in localDb.Dataset where d.DatasetId == geoClientDataset.DatasetId select d)
                        .FirstOrDefault();
                if (dataset == null)
                    return false;

                dataset.MaxCount = geoClientDataset.MaxCount;
                dataset.LastIndex = geoClientDataset.LastIndex;
                dataset.Name = geoClientDataset.Name;
                dataset.ProviderDatasetId = geoClientDataset.ProviderDatasetId;
                dataset.SyncronizationUrl = geoClientDataset.SynchronizationUrl;
                dataset.ClientWfsUrl = geoClientDataset.ClientWfsUrl;
                dataset.TargetNamespace = geoClientDataset.Applicationschema;
                dataset.MappingFile = geoClientDataset.MappingFile;
                dataset.AbortedEndIndex = geoClientDataset.AbortedEndIndex;
                dataset.AbortedTransaction = geoClientDataset.AbortedTransaction;
                dataset.AbortedChangelogPath = geoClientDataset.AbortedChangelogPath;
                dataset.ChangelogDirectory = geoClientDataset.ChangelogDirectory;
                dataset.AbortedChangelogId = geoClientDataset.AbortedChangelogId;
                dataset.UserName = geoClientDataset.UserName;
                if (geoClientDataset.Password != "******")
                    dataset.Password = geoClientDataset.Password;
                dataset.Version = geoClientDataset.Version;

                localDb.SaveChanges();
                return true;
            }
        }

        private static SubscriberDataset MapToGeoClientDataset(Dataset dataset)
        {
            var geoClientDataset = new SubscriberDataset
                                   {
                                       DatasetId = dataset.DatasetId,
                                       Name = dataset.Name,
                                       LastIndex = dataset.LastIndex > 0 ? dataset.LastIndex : -1,
                                       SynchronizationUrl = dataset.SyncronizationUrl,
                                       ClientWfsUrl = dataset.ClientWfsUrl,
                                       MaxCount = dataset.MaxCount> 0 ? dataset.MaxCount : -1,
                                       ProviderDatasetId = dataset.ProviderDatasetId,
                                       Applicationschema = dataset.TargetNamespace,
                                       MappingFile = dataset.MappingFile,
                                       AbortedEndIndex = dataset.AbortedEndIndex,
                                       AbortedTransaction = dataset.AbortedTransaction,
                                       AbortedChangelogPath = dataset.AbortedChangelogPath,
                                       ChangelogDirectory = dataset.ChangelogDirectory,
                                       AbortedChangelogId = dataset.AbortedChangelogId,
                                       UserName = dataset.UserName,
                                       Password = dataset.Password,
                                       Version = dataset.Version
                                   };
            return geoClientDataset;
        }

        //public static int GetNextDatasetID()
        //{
        //    int max = 0;
        //    using (var localDb = new geosyncDBEntities())
        //    {
        //        var res = from d in localDb.Dataset select d.DatasetId;
        //        foreach (Int32 id in res)
        //        {
        //            if (max < id) max = id;
        //        }
        //    }
        //    return max + 1;
        //}

        public static IList<Int32> GetListOfDatasetIDs()
        {
            using (var localDb = new geosyncDBEntities())
            {
                IList<Int32> idList = new List<Int32>();
                var res = from d in localDb.Dataset select d.DatasetId;
                foreach (Int32 id in res)
                {
                    idList.Add(id);
                }
                return idList;
            }
        }

        public static string SyncronizationUrl(Int32 DatasetID)
        {
            using (var localDb = new geosyncDBEntities())
            {
                var res = from d in localDb.Dataset where d.DatasetId == DatasetID select d.SyncronizationUrl;
                if (res.First() != null) return res.First().ToString(); else return "";
            }
        }

        public static string ProviderDatasetId(Int32 DatasetID)
        {
            using (var localDb = new geosyncDBEntities())
            {
                var res = from d in localDb.Dataset where d.DatasetId == DatasetID select d.ProviderDatasetId;
                if (res.First() != null) return res.First().ToString(); else return "";
            }
        }
        public static string MappingFile(Int32 DatasetID)
        {
            using (var localDb = new geosyncDBEntities())
            {
                var res = from d in localDb.Dataset where d.DatasetId == DatasetID select d.MappingFile;
                if (res.First() != null) return res.First().ToString(); else return "";
            }
        }

        public static int GetMaxCount(int datasetID)
        {
            using (var localDb = new geosyncDBEntities())
            {
                var res = (from d in localDb.Dataset where d.DatasetId == datasetID select d.MaxCount).FirstOrDefault();
                return res;
            }
        }
        public static string GetLastIndex(int datasetID)
        {
            using (var localDb = new geosyncDBEntities())
            {
                if (localDb.Dataset == null) return null;

                var res = from d in localDb.Dataset where d.DatasetId == datasetID select d.LastIndex;
                if (res.FirstOrDefault() >  0) return res.First().ToString(); else return "";
            }
        }

        public static string ClientWfsUrl(int datasetID)
        {
            using (var localDb = new geosyncDBEntities())
            {
                var res = from d in localDb.Dataset where d.DatasetId == datasetID select d.ClientWfsUrl;
                if (res.First() != null) return res.First().ToString(); else return "";
            }
        }

        public static List<string> GetDatasetNames()
        {
            using (var localDb = new geosyncDBEntities())
            {
                var res = (from d in localDb.Dataset select d.Name).ToList();
                return res;
            }
        }

        public static IDictionary<int, string> GetDatasetNamesAsDictionary()
        {
            using (var localDb = new geosyncDBEntities())
            {
                var dict = localDb.Dataset.Select( t => new { t.DatasetId, t.Name } )
                   .ToDictionary( t => t.DatasetId, t => t.Name );
                return dict;
            }
        }
        public static string TargetNamespace(Int32 DatasetID)
        {
            using (var localDb = new geosyncDBEntities())
            {
                var res = from d in localDb.Dataset where d.DatasetId == DatasetID select d.TargetNamespace;
                if (res.First() != null) return res.First().ToString(); else return "";
            }
        }

        public static bool AddDatasets(IBindingList datasetBindingList, IList<int> selectedDatasets, string providerUrl, string UserName, string Password)
        {
            using (var localDb = new geosyncDBEntities())
            {
                foreach (int selected in selectedDatasets)
                {
                    var ds = (Dataset)datasetBindingList[selected];
                    try
                    {
                        ds.LastIndex = 0;
                        ds.ClientWfsUrl = "";
                        ds.UserName = UserName;
                        ds.Password = Password;
                        ds.SyncronizationUrl = providerUrl;
                        localDb.AddObject(ds);
                        localDb.SaveChanges();
                    }
                    catch (Exception ex)
                    {
                        logger.LogException(LogLevel.Error, "Error saving selected datasets!", ex);
                        return false;
                    }
                }
            }

            return true;
        }

        public static bool AddEmptyDataset()
        {
            using (var localDb = new geosyncDBEntities())
            {
                    var ds = new Dataset();
                    try
                    {
                        ds.LastIndex = 0;
                        ds.ClientWfsUrl = "";
                        localDb.AddObject(ds);
                        localDb.SaveChanges();
                    }
                    catch (Exception ex)
                    {
                        logger.LogException(LogLevel.Error, "Error saving selected datasets!", ex);
                        return false;
                    }
                }
            
            return true;
        }

        /// <summary>
        /// Removes the selected datasets from database.
        /// </summary>
        /// <param name="datasetBindingList">The dataset binding list.</param>
        /// <param name="selectedDatasets">The selected datasets.</param>
        /// <returns>True if OK, else False</returns>
        public static bool RemoveDatasets(List<SubscriberDataset> datasetBindingList, IList<int> selectedDatasets)
        {
            using (var localDb = new geosyncDBEntities())
            {
                foreach (int selected in selectedDatasets)
                {

                    var geoClientDataset = (SubscriberDataset)datasetBindingList[selected];

                    Dataset dataset =
                         (from d in localDb.Dataset where d.DatasetId == geoClientDataset.DatasetId select d)
                             .FirstOrDefault();
                    if (dataset == null)
                        return false;
                    try
                    {
                        localDb.DeleteObject(dataset);
                        localDb.SaveChanges();
                    }
                    catch (Exception ex)
                    {
                        logger.LogException(LogLevel.Error, "Error removing selected datasets!", ex);
                        return false;
                    }
        
                }
            }

            return true;
        }

   
    }

    public class Dataset
    {
        public string ProviderDatasetId { get; set; }
        public string Name { get; set; }
        public int MaxCount { get; set; }
        //public string Applicationschema { get; set; }
        public string TargetNamespace { get; set; }
        
        public string SyncronizationUrl { get; set; }

        [Key]
        public int DatasetId { get; set; }
        public long LastIndex { get; set; }
        public string ClientWfsUrl { get; set; }
        public string UserName { get; set; }
        public string Password { get; set; }
        public string MappingFile { get; set; }
        public long? AbortedEndIndex { get; set; }
        public long? AbortedTransaction { get; set; }
        public string AbortedChangelogPath { get; set; }
        public string ChangelogDirectory { get; set; }
        public string AbortedChangelogId { get; set; }
        public string Version { get; set; }
    }
}
