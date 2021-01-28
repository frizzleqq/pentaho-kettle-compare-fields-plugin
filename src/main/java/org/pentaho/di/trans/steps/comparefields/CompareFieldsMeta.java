package org.pentaho.di.trans.steps.comparefields;

import java.util.ArrayList;
import java.util.List;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;
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
  image = "org/pentaho/di/trans/steps/comparefields/resources/CompareFields.svg",
  isSeparateClassLoaderNeeded = false )
public class CompareFieldsMeta extends BaseStepMeta implements StepMetaInterface {

  private static Class<?> PKG = CompareFieldsMeta.class; // for i18n purposes, needed by Translator2!!

  private static final String CHANGED_TARGET_STEP = "changed_target_step";
  private static final String IDENTICAL_TARGET_STEP = "identical_target_step";
  private static final String ADDED_TARGET_STEP = "added_target_step";
  private static final String REMOVED_TARGET_STEP = "removed_target_step";
  private static final String ADD_FIELDS_LIST = "add_fields_list";
  private static final String FIELDS_LIST_FIELD = "fields_list_field";

  private static final String XML_TAG_FIELDS = "fields";

  private List<CompareField> compareFields;

  private String identicalTargetStepname;
  private StepMeta identicalTargetStep;
  private String changedTargetStepname;
  private StepMeta changedTargetStep;
  private String addedTargetStepname;
  private StepMeta addedTargetStep;
  private String removedTargetStepname;
  private StepMeta removedTargetStep;

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
  public void getFields( RowMetaInterface inputRowMeta, String name, RowMetaInterface[] info, StepMeta nextStep, VariableSpace space, Repository repository, IMetaStore metaStore ) throws KettleStepException {

    // See if we need to add the list of changed fields...
    //
    if ( addingFieldsList && !Const.isEmpty( fieldsListFieldname ) ) {
      try {
        ValueMetaInterface fieldsList = ValueMetaFactory.createValueMeta( fieldsListFieldname, ValueMetaInterface.TYPE_STRING );
        inputRowMeta.addValueMeta( fieldsList );
      } catch ( KettlePluginException e ) {
        throw new KettleStepException( "Unable to create new String value metadata object", e );
      }
    }
  }

  @Override
  public String getXML() throws KettleException {
    StringBuilder xml = new StringBuilder();

    xml.append( XMLHandler.addTagValue( CHANGED_TARGET_STEP, getStepname( changedTargetStep ) ) );
    xml.append( XMLHandler.addTagValue( IDENTICAL_TARGET_STEP, getStepname( identicalTargetStep ) ) );
    xml.append( XMLHandler.addTagValue( ADDED_TARGET_STEP, getStepname( addedTargetStep ) ) );
    xml.append( XMLHandler.addTagValue( REMOVED_TARGET_STEP, getStepname( removedTargetStep ) ) );

    xml.append( XMLHandler.addTagValue( ADD_FIELDS_LIST, addingFieldsList ) );
    xml.append( XMLHandler.addTagValue( FIELDS_LIST_FIELD, fieldsListFieldname ) );

    xml.append( XMLHandler.openTag( XML_TAG_FIELDS ) );
    for ( int i = 0; i < compareFields.size(); i++ ) {
      xml.append( compareFields.get( i ).getXML() );
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
  public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore ) throws KettleXMLException {

    changedTargetStepname = XMLHandler.getTagValue( stepnode, CHANGED_TARGET_STEP );
    identicalTargetStepname = XMLHandler.getTagValue( stepnode, IDENTICAL_TARGET_STEP );
    addedTargetStepname = XMLHandler.getTagValue( stepnode, ADDED_TARGET_STEP );
    removedTargetStepname = XMLHandler.getTagValue( stepnode, REMOVED_TARGET_STEP );

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

    rep.saveStepAttribute( transformationId, stepId, CHANGED_TARGET_STEP, getStepname( changedTargetStep ) );
    rep.saveStepAttribute( transformationId, stepId, IDENTICAL_TARGET_STEP, getStepname( identicalTargetStep ) );
    rep.saveStepAttribute( transformationId, stepId, ADDED_TARGET_STEP, getStepname( addedTargetStep ) );
    rep.saveStepAttribute( transformationId, stepId, REMOVED_TARGET_STEP, getStepname( removedTargetStep ) );

    rep.saveStepAttribute( transformationId, stepId, ADD_FIELDS_LIST, addingFieldsList );
    rep.saveStepAttribute( transformationId, stepId, FIELDS_LIST_FIELD, fieldsListFieldname );

    for ( int i = 0; i < compareFields.size(); i++ ) {
      compareFields.get( i ).saveRep( rep, transformationId, stepId, i );
    }
  }

  @Override
  public void readRep( Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases )
    throws KettleException {

    changedTargetStepname = rep.getStepAttributeString( id_step, CHANGED_TARGET_STEP );
    identicalTargetStepname = rep.getStepAttributeString( id_step, IDENTICAL_TARGET_STEP );
    addedTargetStepname = rep.getStepAttributeString( id_step, ADDED_TARGET_STEP );
    removedTargetStepname = rep.getStepAttributeString( id_step, REMOVED_TARGET_STEP );

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
    identicalTargetStep = StepMeta.findStep( steps, identicalTargetStepname );
    changedTargetStep = StepMeta.findStep( steps, changedTargetStepname );
    addedTargetStep = StepMeta.findStep( steps, addedTargetStepname );
    removedTargetStep = StepMeta.findStep( steps, removedTargetStepname );

    resetStepIoMeta();
  }

  private static StreamInterface newIdenticalStream = new Stream( StreamType.TARGET, null, BaseMessages.getString(
    PKG, "CompareFieldsMeta.TargetStream.Identical.Description" ), StreamIcon.TARGET, null );
  private static StreamInterface newChangedStream = new Stream( StreamType.TARGET, null, BaseMessages.getString(
    PKG, "CompareFieldsMeta.TargetStream.Changed.Description" ), StreamIcon.TARGET, null );
  private static StreamInterface newAddedStream = new Stream( StreamType.TARGET, null, BaseMessages.getString(
    PKG, "CompareFieldsMeta.TargetStream.Added.Description" ), StreamIcon.TARGET, null );
  private static StreamInterface newRemovedStream = new Stream( StreamType.TARGET, null, BaseMessages.getString(
    PKG, "CompareFieldsMeta.TargetStream.Removed.Description" ), StreamIcon.TARGET, null );

  public List<StreamInterface> getOptionalStreams() {
    List<StreamInterface> list = new ArrayList<StreamInterface>();

    if ( getIdenticalTargetStep() == null ) {
      list.add( newIdenticalStream );
    }
    if ( getChangedTargetStep() == null ) {
      list.add( newChangedStream );
    }
    if ( getAddedTargetStep() == null ) {
      list.add( newAddedStream );
    }
    if ( getRemovedTargetStep() == null ) {
      list.add( newRemovedStream );
    }

    return list;
  }

  public void handleStreamSelection( StreamInterface stream ) {
    if ( stream == newIdenticalStream ) {
      setIdenticalTargetStep( stream.getStepMeta() );
    }
    if ( stream == newChangedStream ) {
      setChangedTargetStep( stream.getStepMeta() );
    }
    if ( stream == newAddedStream ) {
      setAddedTargetStep( stream.getStepMeta() );
    }
    if ( stream == newRemovedStream ) {
      setRemovedTargetStep( stream.getStepMeta() );
    }

    resetStepIoMeta(); // force stepIo to be recreated when it is next needed.
  }

  @Override
  public StepIOMetaInterface getStepIOMeta() {
    StepIOMetaInterface ioMeta = super.getStepIOMeta( false );
    if ( ioMeta == null ) {
      ioMeta = new StepIOMeta( true, false, false, false, false, true );

      if ( identicalTargetStep != null ) {
        ioMeta.addStream( new Stream( StreamType.TARGET, identicalTargetStep,
          BaseMessages.getString( PKG, "CompareFieldsMeta.TargetStream.Identical.Description" ),
          StreamIcon.TARGET, null ) );
      }
      if ( changedTargetStep != null ) {
        ioMeta.addStream( new Stream( StreamType.TARGET, changedTargetStep,
          BaseMessages.getString( PKG, "CompareFieldsMeta.TargetStream.Changed.Description" ),
          StreamIcon.TARGET, null ) );
      }
      if ( addedTargetStep != null ) {
        ioMeta.addStream( new Stream( StreamType.TARGET, addedTargetStep,
          BaseMessages.getString( PKG, "CompareFieldsMeta.TargetStream.Added.Description" ),
          StreamIcon.TARGET, null ) );
      }
      if ( removedTargetStep != null ) {
        ioMeta.addStream( new Stream( StreamType.TARGET, removedTargetStep,
          BaseMessages.getString( PKG, "CompareFieldsMeta.TargetStream.Removed.Description" ),
          StreamIcon.TARGET, null ) );
      }

    }

    return ioMeta;
  }

  /**
   * This method is added to exclude certain steps from copy/distribute checking.
   *
   * @since 4.0.0
   */
  public boolean excludeFromCopyDistributeVerification() {
    return true;
  }

  public List<CompareField> getCompareFields() {
    return compareFields;
  }

  public void setCompareFields( List<CompareField> compareFields ) {
    this.compareFields = compareFields;
  }

  public String getChangedTargetStepname() {
    return changedTargetStepname;
  }

  public void setChangedTargetStepname( String changedTargetStepname ) {
    this.changedTargetStepname = changedTargetStepname;
  }

  public String getIdenticalTargetStepname() {
    return identicalTargetStepname;
  }

  public void setIdenticalTargetStepname( String identicalTargetStepname ) {
    this.identicalTargetStepname = identicalTargetStepname;
  }

  public String getAddedTargetStepname() {
    return addedTargetStepname;
  }

  public void setAddedTargetStepname( String addedTargetStepname ) {
    this.addedTargetStepname = addedTargetStepname;
  }

  public String getRemovedTargetStepname() {
    return removedTargetStepname;
  }

  public void setRemovedTargetStepname( String removedTargetStepname ) {
    this.removedTargetStepname = removedTargetStepname;
  }

  public boolean isAddingFieldsList() {
    return addingFieldsList;
  }

  public void setAddingFieldsList( boolean addingFieldsList ) {
    this.addingFieldsList = addingFieldsList;
  }

  public StepMeta getChangedTargetStep() {
    return changedTargetStep;
  }

  public void setChangedTargetStep( StepMeta changedTargetStep ) {
    this.changedTargetStep = changedTargetStep;
  }

  public StepMeta getIdenticalTargetStep() {
    return identicalTargetStep;
  }

  public void setIdenticalTargetStep( StepMeta identicalTargetStep ) {
    this.identicalTargetStep = identicalTargetStep;
  }

  public StepMeta getAddedTargetStep() {
    return addedTargetStep;
  }

  public void setAddedTargetStep( StepMeta addedTargetStep ) {
    this.addedTargetStep = addedTargetStep;
  }

  public StepMeta getRemovedTargetStep() {
    return removedTargetStep;
  }

  public void setRemovedTargetStep( StepMeta removedTargetStep ) {
    this.removedTargetStep = removedTargetStep;
  }

  public String getFieldsListFieldname() {
    return fieldsListFieldname;
  }

  public void setFieldsListFieldname( String fieldsListFieldname ) {
    this.fieldsListFieldname = fieldsListFieldname;
  }
}
