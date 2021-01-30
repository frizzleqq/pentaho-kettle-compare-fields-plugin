package org.pentaho.di.trans.steps.comparefields;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepIOMeta;
import org.pentaho.di.trans.step.StepIOMetaInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.step.errorhandling.Stream;
import org.pentaho.di.trans.step.errorhandling.StreamIcon;
import org.pentaho.di.trans.step.errorhandling.StreamInterface;
import org.pentaho.di.trans.step.errorhandling.StreamInterface.StreamType;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

@Step(
  id = "CompareFields",
  name = "CompareFields.Name",
  description = "CompareFields.Description",
  casesUrl = "jira.pentaho.com/browse/PDI",
  documentationUrl = "http://wiki.pentaho.com/display/EAI",
  i18nPackageName = "org.pentaho.di.trans.steps.comparefields",
  categoryDescription = "i18n:org.pentaho.di.trans.step:BaseStep.Category.Flow",
  forumUrl = "forums.pentaho.com/forumdisplay.php?135-Pentaho-Data-Integration-Kettle",
  image = "org/pentaho/di/trans/steps/comparefields/resources/CompareFields.svg"
)
public class CompareFieldsMeta extends BaseStepMeta implements StepMetaInterface {

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

  private List<CompareField> compareFields;

  private boolean addingFieldsList;
  private String fieldsListFieldname;

  public CompareFieldsMeta() {
    super();

    compareFields = new ArrayList<CompareField>();
  }

  @Override
  public void setDefault() {
  }

  @Override
  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr,
    TransMeta transMeta, Trans trans ) {
    return new CompareFields( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  @Override
  public StepDataInterface getStepData() {
    return new CompareFieldsData();
  }

  @Override
  public void getFields( RowMetaInterface inputRowMeta, String name, RowMetaInterface[] info, StepMeta nextStep,
                         VariableSpace space, Repository repository, IMetaStore metaStore ) throws KettleStepException {

    // See if we need to add the list of changed fields...
    //
    if ( addingFieldsList && !Utils.isEmpty( fieldsListFieldname ) ) {
      try {
        ValueMetaInterface fieldsList = ValueMetaFactory.createValueMeta( fieldsListFieldname, ValueMetaInterface.TYPE_STRING );
        inputRowMeta.addValueMeta( fieldsList );
      } catch ( KettlePluginException e ) {
        throw new KettleStepException( "Unable to create new String value metadata object", e );
      }
    }
  }

  @Override
  public String getXML() {
    StringBuilder xml = new StringBuilder();

    xml.append( XMLHandler.addTagValue( IDENTICAL_TARGET_STEP, getIdenticalTargetStepname() ) );
    xml.append( XMLHandler.addTagValue( CHANGED_TARGET_STEP, getChangedTargetStepname() ) );
    xml.append( XMLHandler.addTagValue( ADDED_TARGET_STEP, getAddedTargetStepname() ) );
    xml.append( XMLHandler.addTagValue( REMOVED_TARGET_STEP, getRemovedTargetStepname() ) );

    xml.append( XMLHandler.addTagValue( ADD_FIELDS_LIST, addingFieldsList ) );
    xml.append( XMLHandler.addTagValue( FIELDS_LIST_FIELD, fieldsListFieldname ) );

    xml.append( XMLHandler.openTag( XML_TAG_FIELDS ) );
    for (CompareField compareField : compareFields) {
      xml.append(compareField.getXML());
    }
    xml.append( XMLHandler.closeTag( XML_TAG_FIELDS ) );

    return xml.toString();
  }

  private String getStepname( StepMeta stepMeta ) {
    if ( stepMeta == null ) {
      return null;
    }
    return stepMeta.getName();
  }

  @Override
  public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore ) {

    setIdenticalTargetStepname( XMLHandler.getTagValue( stepnode, IDENTICAL_TARGET_STEP ) );
    setChangedTargetStepname( XMLHandler.getTagValue( stepnode, CHANGED_TARGET_STEP ) );
    setAddedTargetStepname( XMLHandler.getTagValue( stepnode, ADDED_TARGET_STEP ) );
    setRemovedTargetStepname( XMLHandler.getTagValue( stepnode, REMOVED_TARGET_STEP ) );

    addingFieldsList = "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, ADD_FIELDS_LIST ) );
    fieldsListFieldname = XMLHandler.getTagValue( stepnode, FIELDS_LIST_FIELD );

    compareFields = new ArrayList<CompareField>();

    Node fieldsNode = XMLHandler.getSubNode( stepnode, XML_TAG_FIELDS );
    List<Node> fieldNodes = XMLHandler.getNodes( fieldsNode, CompareField.XML_TAG );
    for ( Node fieldNode : fieldNodes ) {
      CompareField field = new CompareField( fieldNode );
      compareFields.add( field );
    }
  }

  @Override
  public void saveRep( Repository rep, IMetaStore metaStore, ObjectId transformationId, ObjectId stepId )
    throws KettleException {

    rep.saveStepAttribute( transformationId, stepId, IDENTICAL_TARGET_STEP, getIdenticalTargetStepname() );
    rep.saveStepAttribute( transformationId, stepId, CHANGED_TARGET_STEP, getChangedTargetStepname() );
    rep.saveStepAttribute( transformationId, stepId, ADDED_TARGET_STEP, getAddedTargetStepname() );
    rep.saveStepAttribute( transformationId, stepId, REMOVED_TARGET_STEP, getRemovedTargetStepname() );

    rep.saveStepAttribute( transformationId, stepId, ADD_FIELDS_LIST, addingFieldsList );
    rep.saveStepAttribute( transformationId, stepId, FIELDS_LIST_FIELD, fieldsListFieldname );

    for ( int i = 0; i < compareFields.size(); i++ ) {
      compareFields.get( i ).saveRep( rep, transformationId, stepId, i );
    }
  }

  @Override
  public void readRep( Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases )
    throws KettleException {

    setIdenticalTargetStepname( rep.getStepAttributeString( id_step, IDENTICAL_TARGET_STEP ) );
    setChangedTargetStepname( rep.getStepAttributeString( id_step, CHANGED_TARGET_STEP ) );
    setAddedTargetStepname( rep.getStepAttributeString( id_step, ADDED_TARGET_STEP ) );
    setRemovedTargetStepname( rep.getStepAttributeString( id_step, REMOVED_TARGET_STEP ) );

    addingFieldsList = rep.getStepAttributeBoolean( id_step, ADD_FIELDS_LIST );
    fieldsListFieldname = rep.getStepAttributeString( id_step, FIELDS_LIST_FIELD );

    int nrFields = rep.countNrStepAttributes( id_step, CompareField.CODE_COMPARE_FIELD );
    for ( int fieldNr = 0; fieldNr < nrFields; fieldNr++ ) {
      CompareField field = new CompareField( rep, id_step, fieldNr );
      compareFields.add( field );
    }
  }

  @Override
  public void searchInfoAndTargetSteps( List<StepMeta> steps ) {
    List<StreamInterface> targetStreams = getStepIOMeta().getTargetStreams();
    for ( StreamInterface stream : targetStreams ) {
      stream.setStepMeta( StepMeta.findStep( steps, (String) stream.getSubject() ) );
    }
  }

  /**
   * Returns the Input/Output metadata for this step.
   */
  public StepIOMetaInterface getStepIOMeta() {
    StepIOMetaInterface ioMeta = super.getStepIOMeta( false );
    if ( ioMeta == null ) {
      ioMeta = new StepIOMeta( true, false, false, false,
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

      setStepIOMeta(ioMeta);

    }

    return ioMeta;
  }

  @Override
  public void resetStepIoMeta() {
    // so this does not reset ioMeta (set StreamTargets null)
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
    StreamInterface stream = getStepIOMeta().getTargetStreams().get( streamIndex );
    return java.util.stream.Stream.of( stream.getStepname(), stream.getSubject() )
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
    getStepIOMeta().getTargetStreams().get( IDENTICAL_TARGET_STREAM ).setSubject( identicalTargetStepname );
  }

  public String getChangedTargetStepname() {
    return getTargetStepName( CHANGED_TARGET_STREAM );
  }

  public void setChangedTargetStepname( String changedTargetStepname ) {
    getStepIOMeta().getTargetStreams().get( CHANGED_TARGET_STREAM ).setSubject( changedTargetStepname );
  }

  public String getAddedTargetStepname() {
    return getTargetStepName( ADDED_TARGET_STREAM );
  }

  public void setAddedTargetStepname( String addedTargetStepname ) {
    getStepIOMeta().getTargetStreams().get( ADDED_TARGET_STREAM ).setSubject( addedTargetStepname );
  }

  public String getRemovedTargetStepname() {
    return getTargetStepName( REMOVED_TARGET_STREAM );
  }

  public void setRemovedTargetStepname( String removedTargetStepname ) {
    getStepIOMeta().getTargetStreams().get( REMOVED_TARGET_STREAM ).setSubject( removedTargetStepname );
  }

  public boolean isAddingFieldsList() {
    return addingFieldsList;
  }

  public void setAddingFieldsList( boolean addingFieldsList ) {
    this.addingFieldsList = addingFieldsList;
  }

  public StepMeta getIdenticalTargetStepMeta() {
    return getStepIOMeta().getTargetStreams().get( IDENTICAL_TARGET_STREAM ).getStepMeta();
  }

  public void setIdenticalTargetStepMeta(StepMeta identicalTargetStepMeta) {
    getStepIOMeta().getTargetStreams().get( IDENTICAL_TARGET_STREAM ).setStepMeta(identicalTargetStepMeta);
  }

  public StepMeta getChangedTargetStepMeta() {
    return getStepIOMeta().getTargetStreams().get( CHANGED_TARGET_STREAM ).getStepMeta();
  }

  public void setChangedTargetStepMeta(StepMeta changedTargetStepMeta) {
    getStepIOMeta().getTargetStreams().get( CHANGED_TARGET_STREAM ).setStepMeta(changedTargetStepMeta);
  }

  public StepMeta getAddedTargetStepMeta() {
    return getStepIOMeta().getTargetStreams().get( ADDED_TARGET_STREAM ).getStepMeta();
  }

  public void setAddedTargetStepMeta(StepMeta addedTargetStepMeta) {
    getStepIOMeta().getTargetStreams().get( ADDED_TARGET_STREAM ).setStepMeta(addedTargetStepMeta);
  }

  public StepMeta getRemovedTargetStepMeta() {
    return getStepIOMeta().getTargetStreams().get( REMOVED_TARGET_STREAM ).getStepMeta();
  }

  public void setRemovedTargetStepMeta(StepMeta removedTargetStepMeta) {
    getStepIOMeta().getTargetStreams().get( REMOVED_TARGET_STREAM ).setStepMeta(removedTargetStepMeta);
  }

  public String getFieldsListFieldname() {
    return fieldsListFieldname;
  }

  public void setFieldsListFieldname( String fieldsListFieldname ) {
    this.fieldsListFieldname = fieldsListFieldname;
  }
}
