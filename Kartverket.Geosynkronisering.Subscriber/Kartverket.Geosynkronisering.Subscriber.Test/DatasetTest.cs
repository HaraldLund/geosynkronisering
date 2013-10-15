﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Kartverket.Geosynkronisering.Subscriber.DL;
using NUnit.Framework;

namespace Kartverket.Geosynkronisering.Subscriber.Test
{
    [TestFixture]
    public class DatasetTest
    {
        [Test]
        public void TestGetDataset()
        {
            var dataset = SubscriberDatasetManager.GetDataset(1); 
            Assert.AreEqual(dataset.Name,"Flytebrygge");
        }
    }
}
