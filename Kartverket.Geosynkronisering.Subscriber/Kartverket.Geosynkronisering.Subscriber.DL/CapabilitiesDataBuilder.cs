﻿using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.ServiceModel;
using Kartverket.GeosyncWCF;

namespace Kartverket.Geosynkronisering.Subscriber.DL
{
    public class CapabilitiesDataBuilder
    {
        public CapabilitiesDataBuilder(string providerUrl, string userName, string password)
        {
            var client = new WebFeatureServiceReplicationPortClient();

            if (client.ClientCredentials != null)
            {
                client.ClientCredentials.UserName.UserName = userName;
                client.ClientCredentials.UserName.Password = password;
            }

            client.Endpoint.Address = new EndpointAddress(providerUrl);

            ReadGetCapabilities(client);
        }

        public IBindingList ProviderDatasets { get; private set; }


        private void ReadGetCapabilities(WebFeatureServiceReplicationPort client)
        {
            var req = new GetCapabilitiesType1();
            var rootCapabilities = client.GetCapabilities(req);

            //Build Cababilities.XML
            //ServiceIndentification
            ProviderDatasets = new BindingList<Dataset>();
            foreach (var dst in rootCapabilities.datasets)
            {
                var precision = client.GetPrecision(dst.datasetId);
                var ds = new Dataset
                {
                    ProviderDatasetId = dst.datasetId.Trim(),
                    Name = dst.name.Trim(),
                    Version = dst.version.Trim(),
                    Tolerance = precision.tolerance,
                    EpsgCode = precision.epsgCode.Trim(),
                    Decimals = precision.decimals.Trim()
                };

                var dt = GetConstraint("CountDefault", rootCapabilities.OperationsMetadata.Constraint);
                if (dt != null) ds.MaxCount = Convert.ToInt32(dt.DefaultValue.Value);
                ds.TargetNamespace = dst.applicationSchema;

                var op = GetOperation("OrderChangelog", rootCapabilities.OperationsMetadata.Operation);
                if (op != null)
                {
                    var postUrl = GetPostUrl(op.DCP);
                    ds.SyncronizationUrl = postUrl;
                }

                ProviderDatasets.Add(ds);
            }
        }

        private static string GetPostUrl(IReadOnlyList<DCP> dcps)
        {
            var dcp = dcps[0];
            RequestMethodType postReq = null;
            var index = 0;
            foreach (var ict in dcp.Item.ItemsElementName)
            {
                if (ict == ItemsChoiceType1.Post) postReq = dcp.Item.Items[index];
                index++;
            }

            if (postReq == null) return "";

            var href = postReq.href;
            if (postReq.href.EndsWith("/")) href = postReq.href.Remove(postReq.href.LastIndexOf("/", StringComparison.Ordinal));

            return href;

        }

        private static DomainType GetConstraint(string constraintName, IReadOnlyList<DomainType> constraints)
        {
            var index = 0;
            var dt = constraints[index];
            while (!string.Equals(dt.name, constraintName, StringComparison.CurrentCultureIgnoreCase) && index < constraints.Count - 1)
            {
                index++;
                dt = constraints[index];
            }

            return string.Equals(dt.name, constraintName, StringComparison.CurrentCultureIgnoreCase) ? dt : null;
        }

        private static Operation GetOperation(string constraintName, IReadOnlyList<Operation> operations)
        {
            var index = 0;
            var op = operations[index];
            while (!string.Equals(op.name, constraintName, StringComparison.CurrentCultureIgnoreCase) && index < operations.Count() - 1)
            {
                index++;
                op = operations[index];
            }

            return string.Equals(op.name, constraintName, StringComparison.CurrentCultureIgnoreCase) ? op : null;
        }
    }
}