using NLog;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Web;
using System.Xml.Linq;
using System.Diagnostics;
using Microsoft.Win32;
using System.Threading;
using System.Text.RegularExpressions;


namespace Kartverket.Geosynkronisering.ChangelogProviders.FME
{
    public class ChangelogFME
    {
        
        private static readonly Logger logger = LogManager.GetCurrentClassLogger(); // NLog for logging (nuget package)

       
        public XElement GetGMLElement(string fileName)
        {
            if (fileName == null || fileName == "") return null;
            StreamReader reader = null;
            try
            {
                reader = new StreamReader(fileName);


                XElement getFeatureResponse = XElement.Load(reader);


                logger.Info("GetFeatureCollectionFromWFS END");
                return getFeatureResponse;
            }
            catch (Exception exp)
            {
                logger.Error(exp,"ChangelogFME:GetGMLElement File:" + fileName + "\r\n" + "GetGMLElement function failed:");
                throw new System.Exception("GetGMLElement function failed", exp);
            }
            finally
            {
                if (reader != null) reader.Close();
                //File.Delete(inputFileName);
                //File.Delete(responseFile);

            }
        }


        public string GetFeatureCollectionFromFME(string nsPrefixTargetNamespace, string nsAppStr, string fmeCMD, List<string> gmlIds, int datasetId, Dictionary<string,string> transactionType = null)
        {
            logger.Info("GetFeatureCollectionFromWFS START");
            XNamespace nsApp = nsAppStr;
            XNamespace nsFes = "http://www.opengis.net/fes/2.0";
            XNamespace nsWfs = "http://www.opengis.net/wfs/2.0";
            XNamespace nsXsi = "http://www.w3.org/2001/XMLSchema-instance";
            
            //
            // NameSpace prefix must be equal to providers prefix, get it from the settings database.
            // i.e. "app" will not work if TargetNamespacePrefix = "kyst" or "ar5"
            //
            //string nsPrefixApp = changeLog.GetPrefixOfNamespace(nsApp);
            //changeLog.GetPrefixOfNamespace(nsApp);
            if (String.IsNullOrWhiteSpace(nsPrefixTargetNamespace))
            {
                nsPrefixTargetNamespace = "app"; //Shouldn't happen, but works with GeoServer, and is compatible with earlier versions of code
            }

            XDocument wfsGetFeatureDocument = new XDocument(
                new XDeclaration("1.0", "utf-8", "yes"),
                new XElement(nsWfs + "GetFeature",
                new XAttribute("version", "2.0.0"),
                new XAttribute("service", "WFS"),
                new XAttribute(XNamespace.Xmlns + nsPrefixTargetNamespace, nsApp),
                new XAttribute(XNamespace.Xmlns + "wfs", nsWfs),
                new XAttribute(XNamespace.Xmlns + "fes", nsFes),
                new XAttribute("resolveDepth", "*")
                // new XElement(nsWfs + "GetFeature", new XAttribute("version", "2.0.0"), new XAttribute("service", "WFS"), new XAttribute(XNamespace.Xmlns + "app", nsApp), new XAttribute(XNamespace.Xmlns + "wfs", nsWfs), new XAttribute(XNamespace.Xmlns + "fes", nsFes)
                )
            );

            PopulateDocumentForGetFeatureRequest(nsPrefixTargetNamespace, gmlIds, wfsGetFeatureDocument, datasetId, transactionType);

            //Run FME Job to get GML
            string inputFileName ="";            
            string responseFile = "";           
            string resultData = "";
            //bool Error = false;
            try
            {
                // HttpWebRequest httpWebRequest = (HttpWebRequest)WebRequest.Create(wfsUrl) as HttpWebRequest;
                // httpWebRequest.Method = "POST";
                // httpWebRequest.ContentType = "text/xml";

                

                
                string workPath = Path.Combine(Path.GetTempPath(), "GDFME") + "\\";
                string GUIDname = Guid.NewGuid().ToString();
                inputFileName = Path.Combine(workPath, GUIDname + ".xml");
                if (!Directory.Exists(workPath)) Directory.CreateDirectory(workPath);                
                StreamWriter writer = new StreamWriter(inputFileName);
                wfsGetFeatureDocument.Save(writer);
                //wfsGetFeatureDocument.Save("C:\\temp\\gvtest_query.xml");
                logger.Debug("GetFeature: " + inputFileName);
                logger.Info("Working with XML file for GetFeatures: " + inputFileName);
              
                writer.Close();


                FMEExecuter fmeRun = new FMEExecuter();
                //return "";
                resultData = fmeRun.RunFMEScriptWithExit(fmeCMD, inputFileName, workPath, GUIDname, out responseFile);
                if (resultData.ToUpper().Contains("TRANSLATION FAILED") || resultData.ToUpper().Contains("A FATAL ERROR HAS OCCURRED."))
                {
                    //Error = true;
                    logger.Error("FME Failed: \r\n\r\n" + resultData);
                    throw new System.Exception("GetFeatureCollectionFromWFS function failed", new Exception("FME Failed! Contact systemoperator!"));
                }
                else logger.Info("FME Log:\r\n" + resultData);
                logger.Info("Working with GMLFile: " + responseFile);

                return responseFile;               
            }
            catch (System.Exception exp)
            {
                //20130821-Leg:Added logging of wfsGetFeatureDocument
                logger.Error(exp,"GetFeatureCollectionFromWFS: wfsGetFeatureDocument:" + resultData + "\r\n" + "GetFeatureCollectionFromWFS function failed:");
                throw new System.Exception("GetFeatureCollectionFromWFS function failed", exp);
            } finally
            {                
                //if (!Error) File.Delete(inputFileName);
                
            }
        }

        private void PopulateDocumentForGetFeatureRequest(string nsPrefixTargetNamespace, List<string> gmlIds, XDocument wfsGetFeatureDocument, int datasetId, Dictionary<string, string> transactionType = null)
        {
            PopulateDocumentForGetFeatureRequest_simple(nsPrefixTargetNamespace, gmlIds, wfsGetFeatureDocument, datasetId, transactionType);          
        }

        /// <summary>
        /// Execute get feature request for each feature
        /// </summary>
        /// <param name="gmlIds">List of gml ids</param>
        /// <param name="typeIdDict">Dictonary containing type and gml id</param>
        /// <param name="wfsGetFeatureDocument"></param>
        private void PopulateDocumentForGetFeatureRequest_simple(string nsPrefixTargetNamespace, List<string> gmlIds, XDocument wfsGetFeatureDocument, int datasetId, Dictionary<string, string> transactionType = null)
        {            
            XNamespace nsFes = "http://www.opengis.net/fes/2.0";
            XNamespace nsWfs = "http://www.opengis.net/wfs/2.0";

            string nsPrefixTargetNamespaceComplete = nsPrefixTargetNamespace + ":";
            if (String.IsNullOrWhiteSpace(nsPrefixTargetNamespace))
            {
                nsPrefixTargetNamespace = "app"; //Shouldn't happen, but works with GeoServer
                nsPrefixTargetNamespaceComplete = "";
            }

            XElement orElement = new XElement("Or");
            string typename = "";

            // 20131101-Leg: ValueReference content has namespace prefix
            string lokalidValrefContent = nsPrefixTargetNamespaceComplete + "identifikasjon/" + nsPrefixTargetNamespaceComplete +
                                           "Identifikasjon/" + nsPrefixTargetNamespaceComplete + "lokalId";
            
            foreach (string gmlId in gmlIds)
            {
                
                int pos = gmlId.IndexOf(".");
                typename = gmlId.Substring(0, pos);
                string localId = gmlId.Substring(pos + 1);
            
                XElement filterElement = new XElement(nsFes + "Filter");

                // 20131101-Leg: ValueReference content has namespace prefix
                if (transactionType != null)
                {
                    var transType = transactionType[gmlId];                    
                    filterElement.Add(new XElement(nsFes + "PropertyIsEqualTo", new XElement(nsFes + "ValueReference", lokalidValrefContent), new XElement(nsFes + "Literal", localId), new XElement(nsFes + "Type", transType)));
                }
                else
                filterElement.Add(new XElement(nsFes + "PropertyIsEqualTo", new XElement(nsFes + "ValueReference", lokalidValrefContent), new XElement(nsFes + "Literal", localId)));
                
                // filterElement.Add(new XElement("PropertyIsEqualTo", new XElement("ValueReference", "identifikasjon/Identifikasjon/lokalId"), new XElement("Literal", localId)));
                
                wfsGetFeatureDocument.Element(nsWfs + "GetFeature").Add(new XElement(nsWfs + "Query", new XAttribute("typeNames", nsPrefixTargetNamespace + ":" + typename), filterElement));               
                
            }                      
        }
      
    }

    public class FMEExecuter
    {
        //PythonScript runner.
        private string m_resultdata = "";
        //private string m_FMEExe = "";
        private string m_fullResultData = "";
        private string m_FMEParameters = "";        

        public event MessageRecieveHandler MessageRecieved;

        private static readonly Logger logger = LogManager.GetCurrentClassLogger(); // NLog for logging (nuget package)

        protected virtual void OnMessageRecieved(MessageEventArgs e)
        {
            if (MessageRecieved != null)
            {
                MessageRecieved(this, e);
            }
        }

        public string FullMessage
        {
            get { return m_fullResultData; }
        }

        public FMEExecuter()
        {
            //string pyPath = Config.GetSettingString(Config.configSetting.python);
            //RegistryKey ArcGIS = Registry.LocalMachine.OpenSubKey(@"SOFTWARE\Wow6432Node\" + pyPath);
            //if (ArcGIS == null)
            //{
            //    ArcGIS = Registry.LocalMachine.OpenSubKey(@"SOFTWARE\" + pyPath);
            //}
            //string InstallPath = (string)ArcGIS.GetValue("");
            //m_pythonExe = InstallPath + "python.exe";

            //m_FMEParameters = " --InputParameterFile \"@in\" --OutGMLFile \"@out\" ";
            m_FMEParameters = " --SourceDataset_XML \"@in\" --DestDataset_GML \"@out\" ";


        }

        private string getScriptPath(string cmd)
        {
            string[] lines = Regex.Split(cmd, "\" \"");
            string scriptPath = lines[1].Substring(0, lines[1].LastIndexOf("\\"));
            logger.Info("ScriptPath: " + scriptPath);
            return scriptPath;
        }

        public string RunFMEScript(string FMECommand, string InputFile, string workPath, out string ResultFile)
        {
            m_resultdata = "";
            m_fullResultData = "";
            Process p = new Process();
            //string currDir = Config.getDllPath();
            //System.IO.Directory.SetCurrentDirectory(currDir);
            //Directory.SetCurrentDirectory(@"D:\Utvikling\Kystverket\Geosynkronisering\FME\app\FME");
            string currentDir = Directory.GetCurrentDirectory();
            Directory.SetCurrentDirectory(getScriptPath(FMECommand));
            string OutputFile = Path.Combine(workPath, Guid.NewGuid().ToString() + ".gml");

            p.StartInfo.FileName = FMECommand;
            m_FMEParameters = m_FMEParameters.Replace("@in", InputFile);
            m_FMEParameters = m_FMEParameters.Replace("@out", OutputFile);
            ResultFile = OutputFile;                     
            p.StartInfo.Arguments = m_FMEParameters;

            p.StartInfo.UseShellExecute = false;
            p.StartInfo.CreateNoWindow = false;
            p.StartInfo.RedirectStandardOutput = true;
            p.StartInfo.ErrorDialog = true;
            //p.EnableRaisingEvents = true;
            p.StartInfo.RedirectStandardError = true;
            p.OutputDataReceived += new DataReceivedEventHandler(OutputHandler);
            p.ErrorDataReceived += new DataReceivedEventHandler(OutputHandler);

            //logger.Info(p.StartInfo.ToString());
            logger.Info("RunFMEScript: " + FMECommand + m_FMEParameters);
            int timeOut = 60;//Convert.ToInt32(Config.GetSettingString(Config.configSetting.scriptTimeOut));
            timeOut *= 60000;
            try
            {
                p.Start();
                p.BeginOutputReadLine();
                p.BeginErrorReadLine();
                p.WaitForExit(timeOut);
                if (!p.HasExited)
                {
                    p.Kill();
                    throw new Exception("Funksjonens tidbruk er oppbrukt. Årsak kan være nettverkshastighet på filoverføring");
                }
            }
            catch (Exception ex)
            {
                //if (Config.GetSettingString(Config.configSetting.test) == "1") return "";
                logger.Error(ex, "RunFMEScript: " + FMECommand + m_FMEParameters
                    + "\r\n Check FME Log \r\n" + "RunFMEScript function failed:");
                throw ex;
            }
            finally
            {
                p.Close();
                Directory.SetCurrentDirectory(currentDir);

            }
            return m_fullResultData;
        }




        private Process m_p;
        private bool eventHandled = false;

        public string RunFMEScriptWithExit(string FMECommand, string InputFile, string workPath, string GUIDname, out string ResultFile)
        {
            m_resultdata = "";
            m_fullResultData = "";
            m_p = new Process();
            //string currDir = Config.getDllPath();
            //System.IO.Directory.SetCurrentDirectory(currDir);
            //Directory.SetCurrentDirectory(@"D:\Utvikling\Kystverket\Geosynkronisering\FME\app\FME");
            string currentDir = Directory.GetCurrentDirectory();
            Directory.SetCurrentDirectory(getScriptPath(FMECommand));
            string OutputFile = Path.Combine(workPath, GUIDname + ".gml");

            m_p.StartInfo.FileName = FMECommand;
            m_FMEParameters = m_FMEParameters.Replace("@in", InputFile);
            m_FMEParameters = m_FMEParameters.Replace("@out", OutputFile);
            ResultFile = OutputFile;            
            m_p.StartInfo.FileName = FMECommand;
            m_p.StartInfo.Arguments = m_FMEParameters;

            m_p.StartInfo.UseShellExecute = false;
            m_p.StartInfo.CreateNoWindow = true;
            m_p.StartInfo.RedirectStandardOutput = true;
            m_p.StartInfo.ErrorDialog = true;
            m_p.StartInfo.RedirectStandardError = true;
            m_p.EnableRaisingEvents = true;
            m_p.OutputDataReceived += new DataReceivedEventHandler(OutputHandlerWithExit);
            m_p.ErrorDataReceived += new DataReceivedEventHandler(OutputHandlerWithExit);
            m_p.Exited += new EventHandler(processExited);
            logger.Info("RunFMEScript: " + FMECommand + m_FMEParameters);
            //logger.Info(m_p.StartInfo.ToString());
            try
            {
                m_p.Start();
                m_p.BeginOutputReadLine();
                m_p.BeginErrorReadLine();

            }
            catch (Exception ex)
            {
                logger.Info("Feil!");
                logger.Info(ex.Message);
                logger.Error(ex,"Feilet:");
                if (ex.InnerException != null)  logger.Info(ex.InnerException,"Feilet:");
                logger.Error(ex, "RunFMEScript: " + FMECommand + m_FMEParameters
                    + "\r\n Check FME Log \r\n" + "RunFMEScript function failed:");
                logger.Error("RunFMEScript: " + FMECommand + m_FMEParameters
                    + "\r\n Check FME Log \r\n" + "RunFMEScript function failed:");
                logger.Error("Failed running FME jobb");
                logger.Error(ex);                
                throw ex;
            }


            eventHandled = false;
            while (!m_p.HasExited && ! eventHandled)
            {
                doWait();
            }
            
            Directory.SetCurrentDirectory(currentDir);
            m_p.Close();
            return m_fullResultData;

        }

        private void doWait()
        {
            // Wait for Exited event, but not more than 10 seconds.

            int elapsedTime = 0;
            const int SLEEP_AMOUNT = 1000;
            while (!eventHandled)
            {
                elapsedTime += SLEEP_AMOUNT;
                if (elapsedTime > 10000)
                {
                    break;
                }
                Thread.Sleep(SLEEP_AMOUNT);
            }
        }

        private void processExited(object sendter, System.EventArgs e)
        {
            if (m_resultdata.ToUpper().Contains("TRANSLATION FAILED") || m_resultdata.ToUpper().Contains("TRANSLATION WAS SUCCESSFUL"))
            {
                eventHandled = true;               
            }
            else
            {
                eventHandled = false;
                doWait();
            }
        }

        private void OutputHandlerWithExit(object sendingProcess, DataReceivedEventArgs outLine)
        {
            if (outLine.Data != null && outLine.Data.Trim() != "")
            {
                m_resultdata = outLine.Data.ToString();
                m_fullResultData += outLine.Data.ToString() + "\r\n";
                OnMessageRecieved(new MessageEventArgs(outLine.Data.ToString()));
            }
        }


        private void OutputHandler(object sendingProcess, DataReceivedEventArgs outLine)
        {
            if (outLine.Data != null)
            {
                m_resultdata = outLine.Data.ToString();
                m_fullResultData += outLine.Data.ToString() + "\r\n";
            }
        }

    }

    public delegate string AsyncRunPythonScript(string script);

    public delegate void MessageRecieveHandler(Object sender, MessageEventArgs e);

    public class MessageEventArgs : EventArgs
    {
        public enum messageType : byte
        {
            error,
            info,
            display,
            warning
        }

        private messageType m_msgType;
        public messageType msgType
        {
            get
            { return m_msgType; }
        }

        private string m_message;
        public string message
        {
            get
            {
                return m_message;
            }
        }


        public MessageEventArgs(string message)
        {
            try
            {
                m_message = message;
                if (message.ToUpper().Contains("WARNING"))
                {
                    m_msgType = messageType.warning;
                }
                else if (message.ToUpper().Contains("ERROR"))
                {
                    m_msgType = messageType.error;
                }
                else if (message.ToUpper().Contains("DISPLAY"))
                {
                    m_msgType = messageType.display;
                    m_message = message.Remove(message.IndexOf("#DISPLAY:"), 10);
                }
                else m_msgType = messageType.info;
            }
            catch (Exception ex)
            {
                m_message = ex.Message;
                m_msgType = messageType.error;
            }
        }


    }


}