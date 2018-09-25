using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Xml;
using System.Xml.Linq;
using System.Xml.XPath;
using Ionic.Zip;
using Kartverket.GeosyncWCF;
using Kartverket.Geosynkronisering.Database;
using NLog;

namespace Kartverket.Geosynkronisering.ChangelogProviders.FME
{
    public abstract class SpatialDbChangelogFME : IChangelogProvider
    {
        protected static readonly Logger Logger = LogManager.GetCurrentClassLogger();
            // NLog for logging (nuget package)

        protected string PDbConnectInfo;
        private string _pNsApp;
        private string _pFMECmd;
        private string _pNsPrefixTargetNamespace;
        protected string PDbSchema;
        private string _pSchemaFileUri;
        private OrderChangelog _currentOrderChangeLog = null;
        private string destFileName;
        private string destPath;
        private string zipFile;
        private string streamFileLocation;
        private string tmpzipFile;
        private string _version;
        private Dictionary<string, List<string>> XMLFileAsDictionary = null;

        public void Intitalize(int datasetId)
        {
            PDbConnectInfo = DatasetsData.DatasetConnection(datasetId);
            _pFMECmd = DatasetsData.TransformationConnection(datasetId);
            _pNsApp = DatasetsData.TargetNamespace(datasetId);
            _pNsPrefixTargetNamespace = DatasetsData.TargetNamespacePrefix(datasetId);
            PDbSchema = DatasetsData.DbSchema(datasetId);
            _pSchemaFileUri = DatasetsData.SchemaFileUri(datasetId);
            _version = DatasetsData.Version(datasetId);
            destFileName = Guid.NewGuid().ToString();
            destPath = Path.Combine(Path.Combine(Path.GetTempPath(), "GDFME"), destFileName) + "\\"; 
            zipFile = destFileName + ".zip";
            streamFileLocation = AppDomain.CurrentDomain.BaseDirectory + "\\Changelogfiles\\" + zipFile;
            tmpzipFile = Path.Combine(Path.Combine(Path.GetTempPath(), "GDFME"), zipFile);
            
        }

        public abstract string GetLastIndex(int datasetId);

        public OrderChangelog GenerateInitialChangelog(int datasetId)
        {
            string downloadUriBase = ServerConfigData.DownloadUriBase().TrimEnd('/');

            using (geosyncEntities db = new geosyncEntities())
            {
                var initialChangelog = (from d in db.StoredChangelogs
                    where d.DatasetId == datasetId && d.StartIndex == 1 && d.Stored == true && d.Status == "finished"
                    orderby d.DateCreated descending
                    select d).FirstOrDefault();
                if (initialChangelog != null && initialChangelog.DownloadUri != null)
                {
                    Uri uri = new Uri(initialChangelog.DownloadUri);
                    ChangelogManager.DeleteFileOnServer(uri);
                    db.StoredChangelogs.DeleteObject(initialChangelog);
                    db.SaveChanges();
                }
            }
            int startIndex = 1; // StartIndex always 1 on initial changelog
            int endIndex = Convert.ToInt32(GetLastIndex(datasetId));
            int count = 1000; // TODO: Get from dataset table
            Logger.Info("GenerateInitialChangelog START");
            StoredChangelog ldbo = new StoredChangelog();
            ldbo.Stored = true;
            ldbo.Status = "queued";
            ldbo.StartIndex = startIndex;

            ldbo.DatasetId = datasetId;
            ldbo.DateCreated = DateTime.Now;

            //TODO make filter 
            //TODO check if similar stored changelog is already done
            using (geosyncEntities db = new geosyncEntities())
            {
                // Store changelog info in database
                db.StoredChangelogs.AddObject(ldbo);

                OrderChangelog resp = new OrderChangelog();
                resp.changelogId = ldbo.ChangelogId.ToString();

                //New thread and do the work....
                // We're coming back to the thread handling later...
                //string sourceFileName = "Changelogfiles/41_changelog.xml";


                Directory.CreateDirectory(destPath);
                // Loop and create xml files
                int i = 1;
                while (i++ <= Convert.ToInt32(Math.Ceiling((double) endIndex/count)))
                {
                    string partFileName = DateTime.Now.Ticks + ".xml";

                    string fullPathWithFile = Path.Combine(destPath, partFileName);
                    MakeChangeLog(startIndex, count, PDbConnectInfo, _pFMECmd, fullPathWithFile, datasetId);
                    startIndex += count;
                    endIndex = Convert.ToInt32(GetLastIndex(datasetId));
                }

                // Save endIndex to database
                ldbo.EndIndex = endIndex;

                // New code to handle FTP download
                ChangeLogHandler chgLogHandler = new ChangeLogHandler(ldbo, Logger);
                string inFile = "";
                try
                {
                    inFile = destPath;
                    chgLogHandler.CreateZipFileFromFolder(inFile, zipFile, destFileName);
                    ldbo.Status = "queued"; 
                    File.Copy(tmpzipFile, streamFileLocation);                    
                    File.Delete(tmpzipFile);
                    ldbo.Status = "finished"; 
                }
                catch (Exception ex)
                {
                    Logger.Error(ex, string.Format("Failed to create or upload file {0}", zipFile));
                    throw ex;
                }

                try
                {
                    string downLoadUri = string.Format(@"{0}/{1}", downloadUriBase, zipFile);

                    ldbo.DownloadUri = downLoadUri;
                }
                catch (Exception ex)
                {
                    Logger.Error(ex,string.Format("Failed to create or upload file {0}", zipFile));
                    throw ex;
                }


                try
                {
                    db.SaveChanges();
                }
                catch (Exception ex)
                {
                    Logger.Error(ex,
                        string.Format(
                            "Failed on SaveChanges, Kartverket.Geosynkronisering.ChangelogProviders.SpatialDbChangelogFME.OrderChangelog startIndex:{0} count:{1} changelogId:{2}",
                            startIndex, count, ldbo.ChangelogId));
                    throw ex;
                }
                Logger.Info(
                    "Kartverket.Geosynkronisering.ChangelogProviders.SpatialDbChangelogFME.OrderChangelog" +
                    " startIndex:{0}" + " count:{1}" + " changelogId:{2}", startIndex, count, ldbo.ChangelogId);

                Logger.Info("GenerateInitialChangelog END");
                return resp;
            }
        }

        public OrderChangelog CreateChangelog(int startIndex, int count, string todoFilter, int datasetId)
        {
            using (geosyncEntities db = new geosyncEntities())
            {
                if (startIndex < 2)
                {
                    
                    var initialChangelog = (from d in db.StoredChangelogs
                        where
                            d.DatasetId == datasetId && d.StartIndex == 1 && d.Stored == true && d.Status == "finished"
                        orderby d.DateCreated descending
                        select d).FirstOrDefault();
                    if (initialChangelog != null)
                    {
                        OrderChangelog resp = new OrderChangelog();
                        resp.changelogId = initialChangelog.ChangelogId.ToString();
                        _currentOrderChangeLog = resp;
                        return resp;
                    }

                }
                Logger.Info("CreateChangelog START");
                ChangelogManager chlmng = new ChangelogManager(db);
                _currentOrderChangeLog = chlmng.CreateChangeLog(startIndex, count, datasetId);
                chlmng.SetStatus(_currentOrderChangeLog.changelogId, ChangelogStatusType.queued);
            }
            return _currentOrderChangeLog;
        }

        public OrderChangelog _OrderChangelog(int startIndex, int count, string todoFilter, int datasetId)
        {
            string downloadUriBase = ServerConfigData.DownloadUriBase().TrimEnd('/');
            using (geosyncEntities db = new geosyncEntities())
            {
                ChangelogManager chlmng = new ChangelogManager(db);
                
                chlmng.SetStatus(_currentOrderChangeLog.changelogId, ChangelogStatusType.working);
                //System.IO.File.Copy(Utils.BaseVirtualAppPath + sourceFileName, Utils.BaseVirtualAppPath + destFileName);
                try
                {
                    if (!Directory.Exists(destPath)) Directory.CreateDirectory(destPath);
                    MakeChangeLog(startIndex, count, PDbConnectInfo, _pFMECmd, destPath + destFileName + ".xml",
                        datasetId);
                }
                catch (Exception ex)
                {
                    chlmng.SetStatus(_currentOrderChangeLog.changelogId, ChangelogStatusType.cancelled);
                    Logger.Error(ex, string.Format("Failed to make Change Log {0}", destPath + destFileName + ".xml"));
                    throw ex;
                }

                // New code to handle FTP download
                ChangeLogHandler chgLogHandler = new ChangeLogHandler(Logger);
                string inFile = "";
                try
                {
                    inFile = destPath;
                    chgLogHandler.CreateZipFileFromFolder(inFile, zipFile, destFileName);
                    File.Copy(tmpzipFile, streamFileLocation);
                    File.Delete(tmpzipFile);
                }
                catch (Exception ex)
                {
                    chlmng.SetStatus(_currentOrderChangeLog.changelogId, ChangelogStatusType.cancelled);
                    Logger.Error(ex, string.Format("Failed to create or upload file {0}", zipFile));
                    throw ex;
                }

                try
                {
                    string downLoadUri = string.Format(@"{0}/{1}", downloadUriBase, zipFile);
                    chlmng.SetStatus(_currentOrderChangeLog.changelogId, ChangelogStatusType.finished);
                        chlmng.SetDownloadURI(_currentOrderChangeLog.changelogId, downLoadUri);
                }
                catch (Exception ex)
                {
                    Logger.Error(ex, string.Format("Failed to create or upload file {0}", zipFile));
                    throw ex;
                }

                Logger.Info(
                    "Kartverket.Geosynkronisering.ChangelogProviders.PostGISChangelog.OrderChangelog" +
                    " startIndex:{0}" + " count:{1}" + " changelogId:{2}", startIndex, count,
                    _currentOrderChangeLog.changelogId);

                Logger.Info("OrderChangelog END");
                return _currentOrderChangeLog;
            }
        }

        public OrderChangelog OrderChangelog(int startIndex, int count, string todoFilter, int datasetId)
        {
            // If startIndex == 1: Check if initital changelog exists
            if (startIndex == 1)
            {
                using (geosyncEntities db = new geosyncEntities())
                {
                    var initialChangelog = (from d in db.StoredChangelogs
                        where
                            d.DatasetId == datasetId && d.StartIndex == 1 && d.Stored == true && d.Status == "finished"
                        orderby d.DateCreated descending
                        select d).FirstOrDefault();

                    if (initialChangelog != null)
                   {
                        OrderChangelog resp = new OrderChangelog();
                        resp.changelogId = initialChangelog.ChangelogId.ToString();
                        return resp;
                    }
                }
            }

            // If initial changelog don't exists or startIndex != 1
            return _OrderChangelog(startIndex, count, todoFilter, datasetId);
        }

        public abstract void MakeChangeLog(int startChangeId, int count, string dbConnectInfo, string wfsUrl,
            string changeLogFileName, int datasetId);
        
        private Dictionary<string, List<string>> CreateDictionaryFromStringFile(string stringFile)
        {            
            if (XMLFileAsDictionary != null) return XMLFileAsDictionary;
            List<string> commaSeparatedStrings = stringFile.Split('\n').ToList();
            XMLFileAsDictionary = new Dictionary<string, List<string>>();
            int ind = 0;
            foreach (string str in commaSeparatedStrings)
            {
                var feature = str.Split(',').Select(v => v).ToList();              
                if (feature.Count>1) XMLFileAsDictionary.Add(string.Format("{0}.{1}", ind.ToString(), feature[0]), feature);
                ind++;
            }
            return XMLFileAsDictionary;
        }

        private void RemoveDeletesFromUpdateInsert(List<string> transaction, List<string> delete)
        {
            int ind = 0;
            List<string> toDelete = new List<string>();
            foreach (string localID in transaction)
            {                               
                if (delete.Contains(localID))
                {
                     toDelete.Add(localID);
                }
            
                ind++;
            }
            if (toDelete.Count > 0) foreach (string del in toDelete) transaction.Remove(del);
        }

        private void RemoveFromListOrChangeToParent(List<string> transaction, string MappingFile)
        {
            //Format
            //lokalid, orgname, replace, newlokalid, newname
            //{311}, fastsjomerke, 0, {311}, fastsjomerkemfundament, fastsjomerke
            var dict = CreateDictionaryFromStringFile(MappingFile);
            List<string> toDelete = new List<string>();
            Dictionary<string, string> toChange = new Dictionary<string, string>();
            int ind = 0;
            foreach (string localID in transaction)
            {
                string key = localID.Substring(localID.IndexOf('.') + 1);

                if (!key.Contains("{")) key = string.Format("{0}.{1}", ind.ToString(), "{" + key + "}");
                if (!key.Contains("{")) key = "{" + key + "}";
                if (dict.ContainsKey(key))
                {
                    List<string> feature = dict[key];
                    if (feature[2] == "2")
                    {
                        toDelete.Add(localID);
                    }
                    else if (feature[2] == "1")
                    {
                        string newID = feature[5].Replace("\r", "") + "." + feature[3].Replace("\r", "").Replace("{", "").Replace("}", "");
                        toChange.Add(localID, newID);
                    }
                }
                ind++;
            }
            if (toDelete.Count > 0) foreach (string del in toDelete) transaction.Remove(del);
            if (toChange.Count >0)
            {
                foreach (string chg in toChange.Keys) transaction[transaction.IndexOf(chg)] = toChange[chg];
            }

        }

        private int GetTransactionType(int ind, ref string gmlid, string MappingFile, ref string tablename, List<string> handledFeature)
        {
            //Format
            //lokalid, orgname, type, newlokalid, newname
            //{311}, fastsjomerke, 0, {311}, fastsjomerkemfundament
            //{311}, fastsjomerke, 1, {311}, fastsjomerke
            //{311}, fastsjomerke, 2, {311}, fastsjomerkemfundament // 2 = allerede slettet.

          

            var dict = CreateDictionaryFromStringFile(MappingFile);
            string key = gmlid;
            string oldTablename = tablename;
            if (!key.Contains("{")) key = string.Format("{0}.{1}", ind.ToString(),"{" + key + "}");
            if (dict.ContainsKey(key))
            {
                List<string> feature = dict[key];
                tablename = feature[4].Replace("\r", "");
                gmlid = feature[3].Replace("\r", "").Replace("{","").Replace("}","");
                if (handledFeature.Contains(gmlid.ToLower()))
                {
                    Logger.Info("Allerde lagt til: " + key + ", " + oldTablename +  ", omkoblet id: " + gmlid + ", Feature: " + tablename);
                    return 2;
                }
                int value = 0;
                int.TryParse(feature[2], out value);
                return value;
            } return 0;
        }


        public string GetStringFromFile(string fileName)
        {
            if (fileName == null || fileName == "") return null;
            StreamReader reader = null;
            try
            {
                reader = new StreamReader(fileName);
                string getFeatureResponse = reader.ReadToEnd();
                Logger.Info("GetStringFromFile END");
                return getFeatureResponse;
            }
            catch (System.Exception exp)
            {
                Logger.Error(exp, "GetStringFromFile File:" + fileName + "\r\n" + "GetStringFromFile function failed:");
                throw new System.Exception("GetStringFromFile function failed", exp);
            }
            finally
            {
                if (reader != null) reader.Close();
                
            }

        }

        private void CreateInsertAndUpdateGML(int count, List<OptimizedChangeLogElement> optimizedChangeLog, string fmeCmd, Int64 endChangeId, int datasetId, ref string GMLFile, ref string Mappingfile, ref int changelogcounter, ref long portionchangelog)
        {
            Logger.Info("CreateInsertAndUpdateGML START");
            string MappingXML = "";
            try
            {
                //Use changelog_empty.xml as basis for responsefile
                //XElement changeLog = XElement.Load(Utils.BaseVirtualAppPath + @"Changelogfiles\changelog_empty.xml");

                XElement changeLog = BuildChangelogRoot(datasetId);

                int counter = 0;
               // List<string> updateGmlIds = new List<string>();
                //List<string> faultTransGmlIds = new List<string>();
                List<string> deleteGmlIds = new List<string>();
                List<string> mappingGMLIds = new List<string>();
                Dictionary<string, string> transactionType = new Dictionary<string, string>();
                List<string> UpdateInsertGMLIds = new List<string>();
                long portionEndIndex = 0;
                long handle = 0;
                for (int i = 0; i < optimizedChangeLog.Count; i++)
                {
                    OptimizedChangeLogElement current = optimizedChangeLog.ElementAt(i);
                    string gmlId = current.GmlId;
                    string transType = current.TransType;
                    long changeId = current.ChangeId;

                    counter++;

                    if ((i + 1) == optimizedChangeLog.Count)
                    {
                        handle = endChangeId;
                    }
                    else
                    {
                        OptimizedChangeLogElement next = optimizedChangeLog.ElementAt(i + 1);
                        handle = next.ChangeId - 1;
                    }
                    portionEndIndex = handle;

                    if (transType == "D")
                    {
                        deleteGmlIds.Add(gmlId);
                        mappingGMLIds.Add(gmlId);
                        transactionType.Add(gmlId, "D");
                        UpdateInsertGMLIds.Add(gmlId);
                    }
                    else if (transType == "U" || transType == "R")
                    {
                        //updateGmlIds.Add(gmlId);
                        mappingGMLIds.Add(gmlId);
                        UpdateInsertGMLIds.Add(gmlId);
                        transactionType.Add(gmlId, "U");
                    }
                    else if (transType == "I")
                    {
                       // insertGmlIds.Add(gmlId);
                        mappingGMLIds.Add(gmlId);
                        UpdateInsertGMLIds.Add(gmlId);
                        transactionType.Add(gmlId, "I"); 
                    }
                    
                    if (counter == count)
                    {                        
                        break;
                    }
                }

                changelogcounter = counter;
                portionchangelog = portionEndIndex;
                //"D:\Program Files\FME\fme.exe" "D:\Utvikling\Kystverket\Geosynkronisering\FME\app\FME\Geosynk_tilbyder_NFS.fmw"                    
                string mappingCommand = fmeCmd.Replace(".fmw", "_M.fmw");
                int len = "fme.exe".Length;
                if (mappingCommand.Contains('"')) len++;

                string script = mappingCommand.Substring(mappingCommand.IndexOf("fme.exe") + len).Replace('"', ' ').Trim();                
                script = script.Substring(0, script.IndexOf(".fmw") + 4);
                ChangelogFME fme = new ChangelogFME();
                if (File.Exists(script))
                {
                    //lokalid, orgname, replace, newlokalid, newname
                    //{311}, fastsjomerke, 0, {311}, fastsjomerkemfundament
                    //{311}, fastsjomerke, 1, {311}, fastsjomerke
                    //deleteMappingfile = "D67F09E4-BBB4-469D-95E2-FFFDEBBEBD82,FastSjomerke,0,D67F09E4-BBB4-469D-95E2-FFFDEBBEBD82,Lyktefundament\n";
                    //deleteMappingfile += "340408ED-2723-4EE0-9461-FFA71F33A9D6,Racon,1,340408ED-2723-4EE0-9461-FFA71F33A9D6,Fundament\n";
                    //MoveToUpdate(deleteGmlIds, updateGmlIds, deleteMappingfile);


                    MappingXML = fme.GetFeatureCollectionFromFME(_pNsPrefixTargetNamespace, _pNsApp, mappingCommand, mappingGMLIds, datasetId, transactionType);
                    if (File.Exists(MappingXML))
                    {
                       Mappingfile = GetStringFromFile(MappingXML);
                       RemoveFromListOrChangeToParent(UpdateInsertGMLIds, Mappingfile);
                       RemoveDeletesFromUpdateInsert(UpdateInsertGMLIds, deleteGmlIds);
                    }
                    else
                        Mappingfile = null;
                }
                else Mappingfile = null;
               
                if (UpdateInsertGMLIds.Count > 0)
                {                                      
                    GMLFile = fme.GetFeatureCollectionFromFME(_pNsPrefixTargetNamespace, _pNsApp, fmeCmd, UpdateInsertGMLIds, datasetId);
                    if (!File.Exists(GMLFile))  GMLFile = "";
                }               
            }
            catch (Exception exp)
            {
                Logger.Error(exp, string.Format("BuildChangeLogFile function failed, working with following file: {0}.", MappingXML));
                throw new Exception(string.Format("BuildChangeLogFile function failed, working with following files: {0}:", MappingXML), exp);
            }
            Logger.Info("CreateInsertAndUpdateGML END");
        }


        private void BuildInsertChangeLogFile(long handle, string MappingFile, List<OptimizedChangeLogElement> optimizedChangeLog, OptimizedChangeLogElement current, List<OptimizedChangeLogElement> inserts, int i,  int counter, int count, XElement InsertGML, XElement changeLog, int datasetId, List<string> handledFeature)
        {
            Logger.Info("BuildInsertChangeLogFile START");
            current.Handle = handle;
            inserts.Add(current);
            var lastElement = (i + 1 == optimizedChangeLog.Count) || (counter == count);
            if (!lastElement)
            {
                OptimizedChangeLogElement next = optimizedChangeLog.ElementAt(i + 1);

                string nextChange = next.TransType;
                string gmlId = next.GmlId;
                if (MappingFile != null)
                {
                    int pos = gmlId.IndexOf(".");
                    string typename = gmlId.Substring(0, pos);
                    string lokalId = gmlId.Substring(pos + 1);
                    int TransactionType = GetTransactionType(i+1, ref lokalId, MappingFile, ref typename, handledFeature);
                    if (TransactionType != 0 || nextChange != "I") //if next is not insert
                    {
                        //Add collected inserts to changelog
                        AddInsertPortionsToChangeLog(inserts, InsertGML, changeLog, datasetId);
                        inserts.Clear();
                    }
                }else if (nextChange != "I")
                {
                    //Add collected inserts to changelog
                    AddInsertPortionsToChangeLog(inserts, InsertGML, changeLog, datasetId);
                    inserts.Clear();
                }

            }
            else
            {
                AddInsertPortionsToChangeLog(inserts, InsertGML, changeLog, datasetId);
                inserts.Clear();
            }
            Logger.Info("BuildInsertChangeLogFile END");
        }

        public void BuildChangeLogFile(int count, List<OptimizedChangeLogElement> optimizedChangeLog, string fmeCmd,
            int startChangeId, Int64 endChangeId,
            string changeLogFileName, int datasetId)
        {
            Logger.Info("BuildChangeLogFile START");
            string GMLFilename = "";            
            string MappingFile = "";
            //Nullstill Mapping Dictionary.
            XMLFileAsDictionary = null;
            int changelogcounter = 0;
            long portionchangelog = 0;        
            try
            {
                CreateInsertAndUpdateGML(count, optimizedChangeLog, fmeCmd, endChangeId, datasetId, ref GMLFilename, ref MappingFile, ref changelogcounter, ref portionchangelog);
                //Use changelog_empty.xml as basis for responsefile
                //XElement changeLog = XElement.Load(Utils.BaseVirtualAppPath + @"Changelogfiles\changelog_empty.xml");

                XElement changeLog = BuildChangelogRoot(datasetId);
                ChangelogFME fme = new ChangelogFME();
                List<string> handledFeature = new List<string>();
                XElement GMLData = null;
                if (GMLFilename != "")
                {
                    GMLData = fme.GetGMLElement(GMLFilename);
                }

                int counter = 0;


                List<OptimizedChangeLogElement> inserts = new List<OptimizedChangeLogElement>();
                long portionEndIndex = 0;
                for (int i = 0; i < optimizedChangeLog.Count; i++)
                {
                    OptimizedChangeLogElement current = optimizedChangeLog.ElementAt(i);
                    string gmlId = current.GmlId;
                    string transType = current.TransType;
                    long changeId = current.ChangeId;
                    long handle = 0;

                    //If next element == lastelement
                    if ((i + 1) == optimizedChangeLog.Count)
                    {
                        handle = endChangeId;
                    }
                    else
                    {
                        OptimizedChangeLogElement next = optimizedChangeLog.ElementAt(i + 1);
                        handle = next.ChangeId - 1;
                    }
                    portionEndIndex = handle;

                    counter++;

                    if (transType == "D")
                    {
                        if (MappingFile == null)
                            AddDeleteToChangeLog(gmlId, handle, changeLog, datasetId);
                        else
                        {
                            int pos = gmlId.IndexOf(".");
                            string oldtypeName = gmlId.Substring(0, pos);
                            string typename = gmlId.Substring(0, pos);
                            string lokalId = gmlId.Substring(pos + 1);
                            int TransactionType = GetTransactionType(i, ref lokalId, MappingFile, ref typename, handledFeature);

                            if (TransactionType == 1 && GMLFilename != "")
                            { 
                                //AddDeleteToChangeLog(typename + ".update." + lokalId, handle, changeLog, datasetId);
                                //AddReplaceToChangeLog(typename + "." + lokalId, handle, GMLData, changeLog, datasetId);
                                AddReplaceToChangeLog(lokalId, handle, GMLData, changeLog, datasetId);
                                Logger.Info("Omkoblet: " + gmlId + ", omkoblet id: " + lokalId + ", Feature: " + typename + ", TransType: " + transType + ", " + TransactionType.ToString());
                            }
                            else if (TransactionType == 0)
                                AddDeleteToChangeLog(typename + "." + lokalId, handle, changeLog, datasetId);
                            else
                                Logger.Info("Ikke sendt: " + gmlId + ", omkoblet id: " + lokalId + ", TransType: " + transType + ", " + TransactionType.ToString());
                            if (TransactionType == 0) handledFeature.Add(lokalId.ToLower());
                        }

                    }
                    else if ((transType == "U" || transType == "R") && GMLFilename !="")
                    {
                        if (MappingFile != null)
                        {
                            int pos = gmlId.IndexOf(".");
                            string oldtypeName = gmlId.Substring(0, pos);
                            string typename = gmlId.Substring(0, pos);
                            string lokalId = gmlId.Substring(pos + 1);
                            int TransactionType = GetTransactionType(i, ref lokalId, MappingFile, ref typename, handledFeature);
                            //gmlId = typename + "." + lokalId;
                           // gmlId = lokalId;
                            if (TransactionType != 2)
                            {
                                AddReplaceToChangeLog(lokalId, handle, GMLData, changeLog, datasetId);
                                handledFeature.Add(lokalId.ToLower());
                            }
                            if (oldtypeName != typename)
                                Logger.Info("Omkoblet: " + gmlId + ", omkoblet id: " + lokalId + ", Feature: " + typename + ", TransType: " + transType + ", " + TransactionType.ToString());
                                
                        }else

                        AddReplaceToChangeLog(gmlId, handle, GMLData, changeLog, datasetId);
                    }
                    else if (transType == "I" && GMLFilename !="")
                    {
                        OptimizedChangeLogElement insertElement = new OptimizedChangeLogElement(current.GmlId, current.TransType, current.ChangeId);

                        if (MappingFile != null)
                        {
                            int pos = gmlId.IndexOf(".");
                            string typename = gmlId.Substring(0, pos);
                            string lokalId = gmlId.Substring(pos + 1);
                            int TransactionType = GetTransactionType(i, ref lokalId, MappingFile, ref typename, handledFeature);

                            if (TransactionType == 1)
                            {
                                //AddDeleteToChangeLog(typename + ".update." + lokalId, handle, changeLog, datasetId);
                                //AddReplaceToChangeLog(typename + "." + lokalId, handle, GMLData, changeLog, datasetId);
                                Logger.Info("Omkoblet: " + gmlId + ", omkoblet id: " + lokalId + ", Feature: " + typename + ", TransType: " + transType + ", " + TransactionType.ToString());
                                AddReplaceToChangeLog(lokalId, handle, GMLData, changeLog, datasetId);
                            }
                            else if (TransactionType == 0)
                            {
                                insertElement.GmlId = typename + "." + lokalId;
                                BuildInsertChangeLogFile(handle, MappingFile, optimizedChangeLog, insertElement, inserts, i, count, counter, GMLData, changeLog, datasetId, handledFeature);
                            }
                            else
                                Logger.Info("Ikke sendt: " + gmlId + ", omkoblet id: " + lokalId + ", Feature: " + typename + ", TransType: " + transType + ", " + TransactionType.ToString());

                            if (TransactionType != 2) handledFeature.Add(lokalId.ToLower());

                        }
                        else BuildInsertChangeLogFile(handle, MappingFile, optimizedChangeLog, insertElement, inserts, i, count, counter, GMLData, changeLog, datasetId, handledFeature);


                    }
                    if (counter == count)
                    {
                        break;
                    }
                }


                    //Update attributes in chlogf:TransactionCollection
                    UpdateRootAttributes(changeLog, counter, startChangeId, portionEndIndex);

                    if (!CheckChangelogHasFeatures(changeLog) && GMLFilename !="")
                    {
                        Exception exp = new Exception("CheckChangelogHasFeatures found 0 features");
                        Logger.Error(exp, "CheckChangelogHasFeatures found 0 features");
                        throw exp;
                    }
                //}
                //else
                //{                    
                //    UpdateRootAttributes(changeLog, changelogcounter, startChangeId, portionchangelog);
                //}
                //store changelog to file
                changeLog.Save(changeLogFileName);
            }
            catch (Exception exp)
            {
                Logger.Error(exp, "BuildChangeLogFile function failed:");
                throw new Exception("BuildChangeLogFile function failed", exp);
            }
            Logger.Info("BuildChangeLogFile END");
        }

        private void AddReferencedFeatureToChangelog(XElement changeLog, XElement parentElement, XElement getFeatureResponse)
        {
           
            XNamespace xlink = "http://www.w3.org/1999/xlink";

            foreach (var childElement in parentElement.Elements())
            {
                var hrefAttribute = childElement.Attribute(xlink + "href");
                if (hrefAttribute != null)
                {
                    var lokalid = hrefAttribute.Value.Split('_')[hrefAttribute.Value.Split('.').Length - 1];                    
                    XElement referencedElement = FetchFeatureByLokalid(lokalid, getFeatureResponse);
                    if (referencedElement == null)
                    {
                         throw new Exception(string.Format("AddReferencedFeatureToChangelog: LokalID {0} not found in GML file. Could be wrong with the mapping FME workbench!", lokalid), null);
                    }
                    changeLog.Add(referencedElement);
                    AddReferencedFeatureToChangelog(changeLog, referencedElement, getFeatureResponse);
                }
                AddReferencedFeatureToChangelog(changeLog, childElement, getFeatureResponse);
            }            
        }

        private XElement FetchFeatureByLokalid(string lokalid, XElement getFeatureResponse)
        {
            //XNamespace nsChlogf = "http://skjema.geonorge.no/standard/geosynkronisering/1.1/endringslogg";
            XNamespace nsApp = _pNsApp;
            string nsPrefixApp = "app";
            XmlNamespaceManager mgr = new XmlNamespaceManager(new NameTable());
                    
            //XmlNamespaceManager mgr = new XmlNamespaceManager(getFeatureResponse.Document.NameTable); // We now have a namespace manager that knows of the namespaces used in your document.
            mgr.AddNamespace(nsPrefixApp, nsApp.NamespaceName);
            string nsPrefixAppComplete = nsPrefixApp + ":";
            string xpathExpressionLokalid = "//" + nsPrefixAppComplete + "identifikasjon/" + nsPrefixAppComplete +
                                            "Identifikasjon[" + nsPrefixAppComplete + "lokalId='" + lokalid +
                                            "']/../..";

            return getFeatureResponse.XPathSelectElement(xpathExpressionLokalid, mgr);
        }

        private void AddInsertPortionsToChangeLog(List<OptimizedChangeLogElement> insertList, XElement gml, XElement changeLog, int datasetId)
        {
            int portionSize = 100;

            List<OptimizedChangeLogElement> insertsListPortion = new List<OptimizedChangeLogElement>();
            int portionCounter = 0;
            foreach (OptimizedChangeLogElement insert in insertList)
            {
                portionCounter++;
                insertsListPortion.Add(insert);
                if (portionCounter%portionSize == 0)
                {
                    AddInsertsToChangeLog(insertsListPortion, gml, changeLog, datasetId);
                    insertsListPortion.Clear();
                }
            }
            if (insertsListPortion.Count() > 0)
            {
                AddInsertsToChangeLog(insertsListPortion, gml, changeLog, datasetId);
            }

        }

        private void AddInsertsToChangeLog(List<OptimizedChangeLogElement> insertsGmlIds, XElement gml, XElement changeLog, int datasetId)
        {
            List<string> typeNames = new List<string>();
           
            

            List<string> gmlIds = new List<string>();

            long handle = 0;
            foreach (OptimizedChangeLogElement insert in insertsGmlIds)
            {
                gmlIds.Add(insert.GmlId.Substring(insert.GmlId.LastIndexOf('.')+1));

                if (insert.Handle > handle)
                    handle = insert.Handle;
            }

            XElement getFeatureResponse = gml;

            //Build inserts for each typename
            XNamespace nsWfs = "http://www.opengis.net/wfs/2.0";
            XNamespace nsChlogf = "http://skjema.geonorge.no/standard/geosynkronisering/1.1/endringslogg";

            XNamespace nsApp = _pNsApp;
            // 20130917-Leg: Fix
            string nsPrefixApp = changeLog.GetPrefixOfNamespace(nsApp);
            XmlNamespaceManager mgr = new XmlNamespaceManager(new NameTable());
            mgr.AddNamespace(nsPrefixApp, nsApp.NamespaceName);
            string nsPrefixAppComplete = nsPrefixApp + ":";

            XElement insertElement = new XElement(nsWfs + "Insert", new XAttribute("handle", handle));

            foreach (string gmlId in gmlIds)
            {
                XElement feature = FetchFeatureByLokalid(gmlId, getFeatureResponse);
                if (feature == null)
                {
                    throw new Exception(string.Format("AddInsertsToChangeLog: LokalID {0} not found in GML file. Could be wrong with the mapping FME workbench!", gmlId), null);
                }
                insertElement.Add(feature);
                AddReferencedFeatureToChangelog(insertElement, feature, getFeatureResponse);
            }
            
            changeLog.Element(nsChlogf + "transactions").Add(insertElement);    
        }

        private void AddDeleteToChangeLog(string gmlId, long handle, XElement changeLog, int datasetId)
        {
            XNamespace nsWfs = "http://www.opengis.net/wfs/2.0";
            XNamespace nsChlogf = "http://skjema.geonorge.no/standard/geosynkronisering/1.1/endringslogg";
            XNamespace nsFes = "http://www.opengis.net/fes/2.0";
            XNamespace nsApp = _pNsApp;
            string nsPrefixApp = changeLog.GetPrefixOfNamespace(nsApp);
            string nsPrefixAppComplete = nsPrefixApp + ":";
            string xpathExpressionLokalid = nsPrefixAppComplete + "identifikasjon/" + nsPrefixAppComplete +
                                            "Identifikasjon/" + nsPrefixAppComplete + "lokalId";

            int pos = gmlId.IndexOf(".");
            string typename = gmlId.Substring(0, pos);
            string lokalId = gmlId.Substring(pos + 1);
            //new XAttribute("inputFormat", "application/gml+xml; version=3.2"), 
           
            XElement deleteElement = new XElement(nsWfs + "Delete", new XAttribute("handle", handle),
                new XAttribute("typeName", nsPrefixApp + ":" + typename),
                //new XAttribute("typeName", "app:" + typename),
                new XAttribute(XNamespace.Xmlns + nsPrefixApp, nsApp));
            //XElement deleteElement = new XElement(nsWfs + "Delete", new XAttribute("handle", transCounter), new XAttribute("typeName", typename),
            //    new XAttribute("inputFormat", "application/gml+xml; version=3.2"));

            //deleteElement.Add(getFeatureResponse.Element(nsWfs + "member").Nodes());
            //Add filter
            // 20121031-Leg: "lokal_id" replaced by "lokalId"
            // 20131015-Leg: Filter ValueReference content with namespace prefix

            deleteElement.Add(new XElement(nsFes + "Filter",
                new XElement(nsFes + "PropertyIsEqualTo",
                    new XElement(nsFes + "ValueReference", xpathExpressionLokalid),
                    new XElement(nsFes + "Literal", lokalId)
                    )
                ));

            changeLog.Element(nsChlogf + "transactions").Add(deleteElement);
        }


        private void AddReplaceToChangeLog(string gmlId, long handle, XElement gml, XElement changeLog, int datasetId)
        {
           
           // ChangelogFME fme = new ChangelogFME();
            XElement getFeatureResponse = gml;
            XNamespace nsWfs = "http://www.opengis.net/wfs/2.0";
            XNamespace nsChlogf = "http://skjema.geonorge.no/standard/geosynkronisering/1.1/endringslogg";
            XNamespace nsFes = "http://www.opengis.net/fes/2.0";

            XNamespace nsApp = _pNsApp;
            // 20130917-Leg: Fix
            string nsPrefixApp = changeLog.GetPrefixOfNamespace(nsApp);
            string nsPrefixAppComplete = nsPrefixApp + ":";
            string xpathExpressionLokalidFilter = nsPrefixAppComplete + "identifikasjon/" + nsPrefixAppComplete +
                                                  "Identifikasjon/" + nsPrefixAppComplete + "lokalId";

          
            XElement feature = FetchFeatureByLokalid(gmlId, getFeatureResponse);
            if (feature == null)
            {
                throw new Exception(string.Format("AddReplaceToChangeLog: LokalID {0} not found in GML file. Could be wrong with the mapping FME workbench!", gmlId), null);
            }
            XElement replaceElement = new XElement(nsWfs + "Replace", new XAttribute("handle", handle));
            replaceElement.Add(feature);
            AddReferencedFeatureToChangelog(replaceElement, feature, getFeatureResponse);
           
            replaceElement.Add(new XElement(nsFes + "Filter",
                new XElement(nsFes + "PropertyIsEqualTo",
                    new XElement(nsFes + "ValueReference", xpathExpressionLokalidFilter),
                    new XElement(nsFes + "Literal", gmlId)
                    )
                ));
            changeLog.Element(nsChlogf + "transactions").Add(replaceElement);
           
        }

        private void UpdateRootAttributes(XElement changeLog, int counter, int startChangeId, Int64 endChangeId)
        {
            changeLog.SetAttributeValue("numberMatched", counter);
            changeLog.SetAttributeValue("numberReturned", counter);
            changeLog.SetAttributeValue("startIndex", startChangeId);
            changeLog.SetAttributeValue("endIndex", endChangeId);
        }

        private XElement BuildChangelogRoot(int datasetId)
        {
            XNamespace nsChlogf = ServiceData.Namespace();
            XNamespace nsApp = _pNsApp;
            XNamespace nsWfs = "http://www.opengis.net/wfs/2.0";
            XNamespace nsXsi = "http://www.w3.org/2001/XMLSchema-instance";
            XNamespace nsGml = "http://www.opengis.net/gml/3.2";

            // 20150407-Leg: Correct xsd location
            // TODO: Should not be hardcoded
            string schemaLocation = nsChlogf.NamespaceName + " " + ServiceData.SchemaLocation();
            schemaLocation += " " + _pNsApp + " " + _pSchemaFileUri;

            //"2001-12-17T09:30:47Z"
            XElement changelogRoot =
                new XElement(nsChlogf + "TransactionCollection",
                    new XAttribute("timeStamp", DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss.ffzzz")),
                    new XAttribute("numberMatched", ""), new XAttribute("numberReturned", ""),
                    new XAttribute("startIndex", ""), new XAttribute("endIndex", ""),
                    new XAttribute(XNamespace.Xmlns + "xsi", nsXsi),
                    new XAttribute(nsXsi + "schemaLocation", schemaLocation),
                    new XAttribute(XNamespace.Xmlns + "chlogf", nsChlogf),
                    new XAttribute(XNamespace.Xmlns + "app", nsApp),
                    new XAttribute(XNamespace.Xmlns + "wfs", nsWfs),
                    new XAttribute(XNamespace.Xmlns + "gml", nsGml)
                    );
            changelogRoot.Add(new XElement(nsChlogf + "transactions", new XAttribute("service", "WFS"),
                new XAttribute("version", "2.0.0")));
            return changelogRoot;
        }

        private bool CheckChangelogHasFeatures(XElement changeLog)
        {
            var reader = changeLog.CreateReader();
            XmlNamespaceManager manager = new XmlNamespaceManager(reader.NameTable);
            manager.AddNamespace("gml", "http://www.opengis.net/gml/3.2");
            //Search recursively for first occurence of attribute gml:id 
            XElement element = changeLog.XPathSelectElement("//*[@gml:id or @typeName][1]", manager);
            if (element == null)
            {
                return false;
            }

            return true;
        }

        public string GetDatasetVersion(int datasetId)
        {
            _version = DatasetsData.Version(datasetId);
            return _version;
        }
    }

    public class OptimizedChangeLogElement
    {
        public string GmlId;
        public string TransType;
        public long ChangeId;
        public long Handle;

        public OptimizedChangeLogElement(string gmlId, string transType, long changeId)
        {
            GmlId = gmlId;
            TransType = transType;
            ChangeId = changeId;
        }
    }

    public class ChangeLogHandler
    {


        private string _mZipFile;
        private string _mWorkingDirectory;
        //string m_changeLog;
        //StoredChangelog m_storedChangelog;
        //geosyncEntities m_db;
        private Logger _mLogger;

        public ChangeLogHandler(StoredChangelog sclog, Logger logger)
        {
            //m_storedChangelog = sclog;
            //m_db = db;
            _mLogger = logger;
            _mWorkingDirectory = Path.Combine(Path.GetTempPath(), "GDFME");

        }

        public ChangeLogHandler(Logger logger)
        {
            _mLogger = logger;
            _mWorkingDirectory = Path.Combine(Path.GetTempPath(), "GDFME");

        }

        public bool CreateZipFile(string infile, string zipFile)
        {
            using (ZipFile zip = new ZipFile())
            {
                _mZipFile = Path.Combine(_mWorkingDirectory, zipFile);
                zip.AddFile(infile, @"\");

                zip.Comment = "Changelog created " + DateTime.Now.ToString("G");
                zip.Save(_mZipFile);
                zip.Dispose();
            }
            return true;
        }

        public bool CreateZipFileFromFolder(string infolder, string zipFile, string toFolder)
        {
            using (ZipFile zip = new ZipFile())
            {
                _mZipFile = Path.Combine(_mWorkingDirectory, zipFile);
                zip.AddDirectory(infolder, @"\" + toFolder + @"\");

                zip.Comment = "Changelog created " + DateTime.Now.ToString("G");
                zip.Save(_mZipFile);
                zip.Dispose();
            }
            return true;
        }
    }
}