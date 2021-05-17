package org.apache.hop.pipeline.transforms.comparefields;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.w3c.dom.Node;

public class CompareField {
  private static Class<?> PKG = CompareField.class; // for i18n purposes, needed by Translator2!!

  public static final String XML_TAG = "field";
  public static final String CODE_REFERENCE_FIELD = "reference_field";
  public static final String CODE_COMPARE_FIELD = "compare_field";
  //public static final String CODE_IGNORE_CASE = "ignore_case";

  @Injection( name = "REFERENCE_FIELD", group = "COMPARE_FIELDS")
  private String referenceFieldname;
  @Injection( name = "COMPARE_FIELD", group = "COMPARE_FIELDS")
  private String compareFieldname;

  public CompareField() {
    super();
  }

  public CompareField( String referenceFieldname, String compareFieldname) {
    super();
    this.referenceFieldname = referenceFieldname;
    this.compareFieldname = compareFieldname;
  }

  public String getXML() {
    StringBuilder xml = new StringBuilder();
    xml.append( XmlHandler.openTag( XML_TAG ) );
    xml.append( XmlHandler.addTagValue( CODE_REFERENCE_FIELD, referenceFieldname ) );
    xml.append( XmlHandler.addTagValue( CODE_COMPARE_FIELD, compareFieldname ) );
    xml.append( XmlHandler.closeTag( XML_TAG ) );
    return xml.toString();
  }

  public CompareField( Node fieldNode ) {
    referenceFieldname = XmlHandler.getTagValue( fieldNode, CODE_REFERENCE_FIELD );
    compareFieldname = XmlHandler.getTagValue( fieldNode, CODE_COMPARE_FIELD );
  }

  private volatile int referenceFieldIndex;
  private volatile int compareFieldIndex;

  public void index( IRowMeta rowMeta ) throws HopException {

    if ( Utils.isEmpty( referenceFieldname ) ) {
      throw new HopException( BaseMessages.getString( PKG, "CompareField.Error.EmptyReferenceField" ) );
    }
    referenceFieldIndex = rowMeta.indexOfValue( referenceFieldname );
    if ( referenceFieldIndex < 0 ) {
      throw new HopException(
        BaseMessages.getString( PKG, "CompareField.Error.ReferenceFieldNotFound", referenceFieldname ) );
    }
    if ( Utils.isEmpty( compareFieldname ) ) {
      throw new HopException(
        BaseMessages.getString( PKG, "CompareField.Error.CompareFieldEmpty", referenceFieldname ) );
    }
    compareFieldIndex = rowMeta.indexOfValue( compareFieldname );
    if ( compareFieldIndex < 0 ) {
      throw new HopException(
        BaseMessages.getString( PKG, "CompareField.Error.CompareFieldNotFound", compareFieldname ) );
    }
  }

  public String getReferenceFieldname() {
    return referenceFieldname;
  }

  public String getCompareFieldname() {
    return compareFieldname;
  }

  public int getReferenceFieldIndex() {
    return referenceFieldIndex;
  }

  public int getCompareFieldIndex() {
    return compareFieldIndex;
  }

}
