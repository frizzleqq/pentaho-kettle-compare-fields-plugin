package org.apache.hop.pipeline.transforms.comparefields;

import org.apache.hop.core.annotations.Transform;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.exception.HopTransformException;
import org.apache.hop.core.exception.HopXmlException;
import org.apache.hop.core.injection.Injection;
import org.apache.hop.core.injection.InjectionDeep;
import org.apache.hop.core.injection.InjectionSupported;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.*;
import org.apache.hop.pipeline.transform.errorhandling.IStream;
import org.apache.hop.pipeline.transform.errorhandling.IStream.StreamType;
import org.apache.hop.pipeline.transform.errorhandling.Stream;
import org.apache.hop.pipeline.transform.errorhandling.StreamIcon;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Transform(
  id = "CompareFields",
  name = "i18n::CompareFields.Name",
  description = "i18n::CompareFields.Description",
  documentationUrl = "http://wiki.pentaho.com/display/EAI",
  categoryDescription = "i18n:org.apache.hop.pipeline.transform:BaseTransform.Category.Flow",
  image = "org/apache/hop/pipeline/transforms/comparefields/resources/CompareFields.svg"
)
@InjectionSupported( localizationPrefix = "CompareFields.Injection.", groups = { "COMPARE_FIELDS" } )
public class CompareFieldsMeta extends BaseTransformMeta implements ITransformMeta<CompareFields, CompareFieldsData> {

  private static Class<?> PKG = CompareFieldsMeta.class; // for i18n purposes, needed by Translator2!!

  private static final String IDENTICAL_TARGET_STEP = "identical_target_step";
  private static final int IDENTICAL_TARGET_STREAM = 0;
  private static final String CHANGED_TARGET_STEP = "changed_target_step";
  private static final int CHANGED_TARGET_STREAM = 1;
  private static final String ADDED_TARGET_STEP = "added_target_step";
  private static final int ADDED_TARGET_STREAM = 2;
  private static final String REMOVED_TARGET_STEP = "removed_target_step";
  private static final int REMOVED_TARGET_STREAM = 3;

  private static final String ADD_FIELDS_LIST = "add_fields_list";
  private static final String FIELDS_LIST_FIELD = "fields_list_field";

  private static final String XML_TAG_FIELDS = "fields";

  @InjectionDeep
  private List<CompareField> compareFields;

  @Injection( name = "ADD_FIELD_LIST" )
  private boolean addingFieldsList;
  @Injection( name = "FIELD_LIST_FIELD_NAME" )
  private String fieldsListFieldname;

  public CompareFieldsMeta() {
    super();

    compareFields = new ArrayList<CompareField>();
  }

  @Override
  public void setDefault() {
  }

  @Override
  public CompareFields createTransform(TransformMeta transformMeta, CompareFieldsData data, int i, PipelineMeta pipelineMeta, Pipeline pipeline) {
    return new CompareFields(transformMeta, this, data, i, pipelineMeta, pipeline);
  }

  @Override
  public CompareFieldsData getTransformData() {
    return new CompareFieldsData();
  }
  
  @Override
  public void getFields(IRowMeta inputRowMeta, String name, IRowMeta[] info, TransformMeta nextStep,
                        IVariables variables, IHopMetadataProvider metadataProvider ) throws HopTransformException {

    // See if we need to add the list of changed fields...
    //
    if ( addingFieldsList && !Utils.isEmpty( fieldsListFieldname ) ) {
      try {
        IValueMeta fieldsList = ValueMetaFactory.createValueMeta( fieldsListFieldname, IValueMeta.TYPE_STRING );
        inputRowMeta.addValueMeta( fieldsList );
      } catch ( HopPluginException e ) {
        throw new HopTransformException( "Unable to create new String value metadata object", e );
      }
    }
  }

  public String getXML() throws HopException {
    StringBuilder xml = new StringBuilder();

    xml.append( XmlHandler.addTagValue( IDENTICAL_TARGET_STEP, getIdenticalTargetStepname() ) );
    xml.append( XmlHandler.addTagValue( CHANGED_TARGET_STEP, getChangedTargetStepname() ) );
    xml.append( XmlHandler.addTagValue( ADDED_TARGET_STEP, getAddedTargetStepname() ) );
    xml.append( XmlHandler.addTagValue( REMOVED_TARGET_STEP, getRemovedTargetStepname() ) );

    xml.append( XmlHandler.addTagValue( ADD_FIELDS_LIST, addingFieldsList ) );
    xml.append( XmlHandler.addTagValue( FIELDS_LIST_FIELD, fieldsListFieldname ) );

    xml.append( XmlHandler.openTag( XML_TAG_FIELDS ) );
    for (CompareField compareField : compareFields) {
      xml.append(compareField.getXML());
    }
    xml.append( XmlHandler.closeTag( XML_TAG_FIELDS ) );

    return xml.toString();
  }

  private String getStepname( TransformMeta stepMeta ) {
    if ( stepMeta == null ) {
      return null;
    }
    return stepMeta.getName();
  }

  @Override
  public void loadXml(Node transformNode, IHopMetadataProvider metadataProvider)
          throws HopXmlException {

    setIdenticalTargetStepname( XmlHandler.getTagValue( transformNode, IDENTICAL_TARGET_STEP ) );
    setChangedTargetStepname( XmlHandler.getTagValue( transformNode, CHANGED_TARGET_STEP ) );
    setAddedTargetStepname( XmlHandler.getTagValue( transformNode, ADDED_TARGET_STEP ) );
    setRemovedTargetStepname( XmlHandler.getTagValue( transformNode, REMOVED_TARGET_STEP ) );

    addingFieldsList = "Y".equalsIgnoreCase( XmlHandler.getTagValue( transformNode, ADD_FIELDS_LIST ) );
    fieldsListFieldname = XmlHandler.getTagValue( transformNode, FIELDS_LIST_FIELD );

    compareFields = new ArrayList<CompareField>();

    Node fieldsNode = XmlHandler.getSubNode( transformNode, XML_TAG_FIELDS );
    List<Node> fieldNodes = XmlHandler.getNodes( fieldsNode, CompareField.XML_TAG );
    for ( Node fieldNode : fieldNodes ) {
      CompareField field = new CompareField( fieldNode );
      compareFields.add( field );
    }
  }

  @Override
  public void searchInfoAndTargetTransforms(List<TransformMeta> transforms) {
    List<IStream> targetStreams = getTransformIOMeta().getTargetStreams();
    for (IStream stream : targetStreams) {
      stream.setTransformMeta(
              TransformMeta.findTransform(transforms, (String) stream.getSubject()));
    }
  }

  /**
   * Returns the Input/Output metadata for this step.
   */
  public ITransformIOMeta getTransformIOMeta() {
    ITransformIOMeta ioMeta = super.getTransformIOMeta( false );
    if ( ioMeta == null ) {
      ioMeta = new TransformIOMeta( true, false, false, false,
              false, true );

      // do not change order of this
      ioMeta.addStream( new Stream( StreamType.TARGET, null,
        BaseMessages.getString( PKG, "CompareFieldsMeta.TargetStream.Identical.Description" ),
        StreamIcon.TARGET, null ) );
      ioMeta.addStream( new Stream( StreamType.TARGET, null,
        BaseMessages.getString( PKG, "CompareFieldsMeta.TargetStream.Changed.Description" ),
        StreamIcon.TARGET, null ) );
      ioMeta.addStream( new Stream( StreamType.TARGET, null,
        BaseMessages.getString( PKG, "CompareFieldsMeta.TargetStream.Added.Description" ),
        StreamIcon.TARGET, null ) );
      ioMeta.addStream( new Stream( StreamType.TARGET, null,
        BaseMessages.getString( PKG, "CompareFieldsMeta.TargetStream.Removed.Description" ),
        StreamIcon.TARGET, null ) );

      setTransformIOMeta(ioMeta);

    }

    return ioMeta;
  }

  @Override
  public void resetTransformIoMeta() {
    // prevent reset, this clears all stream targets
  }

  /**
   * This method is added to exclude certain steps from copy/distribute checking.
   *
   * @since 4.0.0
   */
  public boolean excludeFromCopyDistributeVerification() {
    return true;
  }

  private String getTargetStepName( int streamIndex ) {
    IStream stream = getTransformIOMeta().getTargetStreams().get( streamIndex );
    return java.util.stream.Stream.of( stream.getTransformName(), stream.getSubject() )
            .filter( Objects::nonNull )
            .findFirst().map( Object::toString ).orElse( null );
  }

  public List<CompareField> getCompareFields() {
    return compareFields;
  }

  public void setCompareFields( List<CompareField> compareFields ) {
    this.compareFields = compareFields;
  }

  public String getIdenticalTargetStepname() {
    return getTargetStepName( IDENTICAL_TARGET_STREAM );
  }

  public void setIdenticalTargetStepname( String identicalTargetStepname ) {
    getTransformIOMeta().getTargetStreams().get( IDENTICAL_TARGET_STREAM ).setSubject( identicalTargetStepname );
  }

  public String getChangedTargetStepname() {
    return getTargetStepName( CHANGED_TARGET_STREAM );
  }

  public void setChangedTargetStepname( String changedTargetStepname ) {
    getTransformIOMeta().getTargetStreams().get( CHANGED_TARGET_STREAM ).setSubject( changedTargetStepname );
  }

  public String getAddedTargetStepname() {
    return getTargetStepName( ADDED_TARGET_STREAM );
  }

  public void setAddedTargetStepname( String addedTargetStepname ) {
    getTransformIOMeta().getTargetStreams().get( ADDED_TARGET_STREAM ).setSubject( addedTargetStepname );
  }

  public String getRemovedTargetStepname() {
    return getTargetStepName( REMOVED_TARGET_STREAM );
  }

  public void setRemovedTargetStepname( String removedTargetStepname ) {
    getTransformIOMeta().getTargetStreams().get( REMOVED_TARGET_STREAM ).setSubject( removedTargetStepname );
  }

  public boolean isAddingFieldsList() {
    return addingFieldsList;
  }

  public void setAddingFieldsList( boolean addingFieldsList ) {
    this.addingFieldsList = addingFieldsList;
  }

  public TransformMeta getIdenticalTargetTransformMeta() {
    return getTransformIOMeta().getTargetStreams().get( IDENTICAL_TARGET_STREAM ).getTransformMeta();
  }

  public void setIdenticalTargetTransformMeta(TransformMeta identicalTargetTransformMeta) {
    getTransformIOMeta().getTargetStreams().get( IDENTICAL_TARGET_STREAM ).setTransformMeta(identicalTargetTransformMeta);
  }

  public TransformMeta getChangedTargetTransformMeta() {
    return getTransformIOMeta().getTargetStreams().get( CHANGED_TARGET_STREAM ).getTransformMeta();
  }

  public void setChangedTargetTransformMeta(TransformMeta changedTargetTransformMeta) {
    getTransformIOMeta().getTargetStreams().get( CHANGED_TARGET_STREAM ).setTransformMeta(changedTargetTransformMeta);
  }

  public TransformMeta getAddedTargetTransformMeta() {
    return getTransformIOMeta().getTargetStreams().get( ADDED_TARGET_STREAM ).getTransformMeta();
  }

  public void setAddedTargetTransformMeta(TransformMeta addedTargetTransformMeta) {
    getTransformIOMeta().getTargetStreams().get( ADDED_TARGET_STREAM ).setTransformMeta(addedTargetTransformMeta);
  }

  public TransformMeta getRemovedTargetTransformMeta() {
    return getTransformIOMeta().getTargetStreams().get( REMOVED_TARGET_STREAM ).getTransformMeta();
  }

  public void setRemovedTargetTransformMeta(TransformMeta removedTargetTransformMeta) {
    getTransformIOMeta().getTargetStreams().get( REMOVED_TARGET_STREAM ).setTransformMeta(removedTargetTransformMeta);
  }

  public String getFieldsListFieldname() {
    return fieldsListFieldname;
  }

  public void setFieldsListFieldname( String fieldsListFieldname ) {
    this.fieldsListFieldname = fieldsListFieldname;
  }
}
