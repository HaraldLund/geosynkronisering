namespace Kartverket.Geosynkronisering
{
    
    
    /// <remarks/>
    [System.CodeDom.Compiler.GeneratedCodeAttribute("System.Xml", "4.0.30319.233")]
    [System.SerializableAttribute()]
    [System.Diagnostics.DebuggerStepThroughAttribute()]
    [System.ComponentModel.DesignerCategoryAttribute("code")]
    [System.Xml.Serialization.XmlTypeAttribute(Namespace="http://www.opengis.net/ows/1.1")]
    public partial class DescriptionType
    {
        
        /// <remarks/>
        [System.Xml.Serialization.XmlElementAttribute("Title")]
        public LanguageStringTypeCollection Title;
        
        /// <remarks/>
        [System.Xml.Serialization.XmlElementAttribute("Abstract")]
        public LanguageStringTypeCollection Abstract;
        
        /// <remarks/>
        [System.Xml.Serialization.XmlElementAttribute("Keywords")]
        public KeywordsTypeCollection Keywords;
    }
}
