using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Data;
using System.Linq;
using System.Web;
using Oracle.ManagedDataAccess.Client;

namespace Kartverket.Geosynkronisering.ChangelogProviders.FME
{
    /// <summary>
    /// SQL Server Spatial Provider part
    /// Written by Lars Eggan, NOIS, march 2016.
    /// </summary>
    public class OracleServerFMEChangelog : SpatialDbChangelogFME
    {
        public override string GetLastIndex(int datasetId)
        {
            Int64 endChangeId = 0;
            try
            {
                //Connect to SQL Server  database

                OracleConnection conn = null;
                using (conn = new OracleConnection(PDbConnectInfo))
                {
                    conn.Open();
                    //Get max changelogid
                    endChangeId = GetMaxChangeLogId(conn, datasetId);
                    conn.Close();
                    Logger.Info("OracleServerFMEChangelog.GetLastIndexResponse endChangeId :{0}{1}", "\t", endChangeId);
                }
            }

            catch (System.Exception exp)
            {
                Logger.Error(exp,"GetLastIndex Exception:");
                throw new System.Exception("GetLastIndex function failed", exp);
            }

            return endChangeId.ToString();
        }
        public override void MakeChangeLog(int startChangeId, int count, string dbConnectInfo, string fmeCmd, string changeLogFileName, int datasetId)
        {
            Logger.Info("OracleServerFMEChangelog..MakeChangeLog START");
            try
            {
                //Connect to postgres database
                OracleConnection conn = null;
                conn = new OracleConnection(dbConnectInfo);
                conn.Open();

                //Get max changelogid
                Int64 endChangeId = GetMaxChangeLogId(conn, datasetId);

                //Prepare query against the changelog table in postgres
                OracleCommand command = null;
                PrepareChangeLogQuery(conn, ref command, startChangeId, endChangeId, datasetId);

                List<OptimizedChangeLogElement> optimizedChangeLog = new List<OptimizedChangeLogElement>();
            
                //Execute query against the changelog table and remove unnecessary transactions.
                FillOptimizedChangeLog(ref command, ref optimizedChangeLog, startChangeId);

                //Get features from WFS and add transactions to changelogfile
                BuildChangeLogFile(count, optimizedChangeLog, fmeCmd, startChangeId, endChangeId, changeLogFileName, datasetId);

                conn.Close();
            }
            catch (System.Exception exp)
            {
                Logger.Error(exp, "OracleServerFMEChangelog.MakeChangeLog function failed:");
                throw new System.Exception("MakeChangeLog function failed", exp);
            }
            Logger.Info("OracleServerFMEChangelog.MakeChangeLog END");

        }

        private void FillOptimizedChangeLog(ref OracleCommand command,
            ref List<OptimizedChangeLogElement> optimizedChangeLog, int startChangeId)
        {
            Logger.Info("OracleServerFMEChangelog.FillOptimizedChangeLog START");
            try
            {

                OrderedDictionary tempOptimizedChangeLog = new OrderedDictionary();
                //Fill optimizedChangeLog
                using (OracleDataReader dr = command.ExecuteReader())
                {
                    while (dr.Read())
                    {
                        string gmlId = dr.GetString(0);

                        //gmlId = char.ToUpper(gmlId[0]) + gmlId.Substring(1);

                        string transType = dr.GetString(1);
                        long changelogId = dr.GetInt64(2);
                        string table = dr.GetString(3);


                        OptimizedChangeLogElement optimizedChangeLogElement;
                        if (transType.Equals("D"))
                        {
                            //Remove if inserted or updated earlier in this sequence of transactions
                            if (tempOptimizedChangeLog.Contains(gmlId))
                            {
                                optimizedChangeLogElement = (OptimizedChangeLogElement)tempOptimizedChangeLog[gmlId];
                                string tempTransType = optimizedChangeLogElement.TransType;
                                tempOptimizedChangeLog.Remove(gmlId);
                                if (tempTransType.Equals("U"))
                                {
                                    //Add delete if last operation was update. 
                                    tempOptimizedChangeLog.Add(gmlId, new OptimizedChangeLogElement(gmlId, transType, changelogId));
                                }
                            }
                            else
                            {
                                tempOptimizedChangeLog.Add(gmlId, new OptimizedChangeLogElement(gmlId, transType, changelogId));
                            }
                        }
                        else
                        {
                            if (!tempOptimizedChangeLog.Contains(gmlId))
                            {
                                tempOptimizedChangeLog.Add(gmlId, new OptimizedChangeLogElement(gmlId, transType, changelogId));
                            }
                        }
                    }
                }

                //Fill optimizedChangeLog
                foreach (var item in tempOptimizedChangeLog.Values)
                {
                    optimizedChangeLog.Add((OptimizedChangeLogElement)item);
                }


            }
            catch (Exception exp)
            {

                Logger.Error(exp, "OracleServerFMEChangelog.FillOptimizedChangeLog function failed:");
                throw new System.Exception("FillOptimizedChangeLog function failed", exp);
            }
            Logger.Info("OracleServerFMEChangelog.FillOptimizedChangeLog END");

        }


        private void PrepareChangeLogQuery(OracleConnection conn, ref OracleCommand command, int startChangeId, Int64 endChangeId, int datasetId)
        {
            Logger.Info("OracleServerFMEChangelog.PrepareChangeLogQuery START");
            try
            {
                // 20160302-Leg: SQL server has different syntax from PostGIS
                //string sqlSelectGmlIds = "SELECT tabell + '.' +  CONVERT(nvarchar(50),lokalid), type, endringsid FROM " + PDbSchema + ".endringslogg WHERE endringsid >= @startChangeId AND endringsid <= @endChangeId ORDER BY endringsid";
                
               // string sqlSelectGmlIds = "SELECT tabell || '.' || lokalid, type, endringsid FROM " + PDbSchema + ".endringslogg WHERE endringsid >= :startChangeId AND endringsid <= :endChangeId ORDER BY endringsid";
                string sqlSelectGmlIds = "SELECT tabell || '.' || lokalid, type, endringsid, tabell FROM " + PDbSchema + ".endringslogg WHERE endringsid >= :startChangeId AND endringsid <= :endChangeId ORDER BY endringsid";

                Logger.Info("OracleServerFMEChangelog.PrepareChangeLogQuery sqlSelectGmlIds: {0}", sqlSelectGmlIds);
                Logger.Info("OracleServerFMEChangelog.PrepareChangeLogQuery StartChangeId: {0}, EndChangeId: {1}", startChangeId, endChangeId);

                command = new OracleCommand(sqlSelectGmlIds, conn);
                command.Parameters.Add(new OracleParameter("startChangeId", OracleDbType.Long));
                command.Parameters.Add(new OracleParameter("endChangeId", OracleDbType.Long));

                command.Prepare();

                command.Parameters[0].Value = startChangeId;
                command.Parameters[1].Value = endChangeId;
            }
            catch (System.Exception exp)
            {
                Logger.Error(exp, "OracleServerFMEChangelog.PrepareChangeLogQuery function failed:");
                throw new System.Exception("PrepareChangeLogQuery function failed", exp);
            }
            Logger.Info("OracleServerFMEChangelog.PrepareChangeLogQuery END");
        }


        private Int64 GetMaxChangeLogId(OracleConnection conn, int datasetid)
        {
            Logger.Info("OracleServerFMEChangelog.GetMaxChangeLogId START");
            try
            {
                Int64 endChangeId = 0;

                string sqlSelectMaxChangeLogId = "SELECT COALESCE(MAX(endringsid),0) FROM " + PDbSchema + ".endringslogg";

                OracleCommand cmd = new OracleCommand(sqlSelectMaxChangeLogId, conn);
                OracleDataReader dr = cmd.ExecuteReader();
                dr.Read(); //Only one row
                endChangeId = dr.GetInt64(0);
                dr.Close();

                Logger.Info("OracleServerFMEChangelog.GetMaxChangeLogId END");
                return endChangeId;
            }
            catch (System.Exception exp)
            {
                Logger.Error(exp,"GetMaxChangeLogId function failed:");
                throw new System.Exception("GetMaxChangeLogId function failed", exp);
            }
        }

    }
}