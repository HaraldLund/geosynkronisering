namespace Kartverket.Geosynkronisering
{
    
    
    /// <remarks/>
    [System.CodeDom.Compiler.GeneratedCodeAttribute("System.Xml", "4.0.30319.233")]
    [System.SerializableAttribute()]
    [System.Diagnostics.DebuggerStepThroughAttribute()]
    [System.ComponentModel.DesignerCategoryAttribute("code")]
    [System.Xml.Serialization.XmlTypeAttribute(Namespace="http://www.opengis.net/wfs/2.0")]
    [System.Xml.Serialization.XmlRootAttribute("ValueCollection", Namespace="http://www.opengis.net/wfs/2.0", IsNullable=false)]
    public partial class ValueCollectionType
    {
        
        /// <remarks/>
        [System.Xml.Serialization.XmlElementAttribute("member")]
        public MemberPropertyTypeCollection member;
        
        /// <remarks/>
        public additionalValues additionalValues;
        
        /// <remarks/>
        public truncatedResponse truncatedResponse;
        
        /// <remarks/>
        [System.Xml.Serialization.XmlAttributeAttribute()]
        public System.DateTime timeStamp;
        
        /// <remarks/>
        [System.Xml.Serialization.XmlAttributeAttribute()]
        public string numberMatched;
        
        /// <remarks/>
        [System.Xml.Serialization.XmlAttributeAttribute(DataType="nonNegativeInteger")]
        public string numberReturned;
        
        /// <remarks/>
        [System.Xml.Serialization.XmlAttributeAttribute(DataType="anyURI")]
        public string next;
        
        /// <remarks/>
        [System.Xml.Serialization.XmlAttributeAttribute(DataType="anyURI")]
        public string previous;
    }
}
