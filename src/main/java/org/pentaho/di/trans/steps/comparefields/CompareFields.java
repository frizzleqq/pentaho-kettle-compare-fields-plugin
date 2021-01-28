package org.pentaho.di.trans.steps.comparefields;

/**
 * 
 */

import java.util.ArrayList;
import java.util.List;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.RowSet;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.steps.comparefields.CompareResult.CompareResultType;

/**
 * Compare a list of 2 field each with each other.
 *  
 * @author matt
 *
 */
public class CompareFields extends BaseStep implements StepInterface {

  private static Class<?> PKG = CompareFields.class; // for i18n purposes, needed by Translator2!!

  private CompareFieldsMeta meta;
  private CompareFieldsData data;

  public CompareFields( StepMeta stepMeta, StepDataInterface stepDataInterface,
    int copyNr, TransMeta transMeta, Trans trans ) {

    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );

    meta = (CompareFieldsMeta) getStepMeta().getStepMetaInterface();
    data = (CompareFieldsData) stepDataInterface;
  }

  @Override
  public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {

    // Get one row from previous step(s).
    //
    Object[] row = getRow();

    if ( row == null ) {
      setOutputDone();
      return false;
    }

    if ( first ) {
      first = false;

      // determine the output fields...
      //
      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields( data.outputRowMeta, getStepname(), null, null, this, repository, metaStore );

      if ( meta.getCompareFields().isEmpty() ) {
        throw new KettleException( BaseMessages.getString( PKG, "CompareFields.Error.NoFieldsToCompare" ) );
      }

      // Index the compare fields...
      //
      for ( CompareField field : meta.getCompareFields() ) {
        field.index( getInputRowMeta() );
      }

      if ( meta.getIdenticalTargetStep() != null ) {
        data.identicalRowSet = findOutputRowSet( meta.getIdenticalTargetStep().getName() );
        if ( data.identicalRowSet == null ) {
          throw new KettleException(
            BaseMessages.getString( PKG, "CompareFields.Error.UnableToFindIdenticalOutputStep",
              meta.getIdenticalTargetStep().getName() ) );
        }
      }
      if ( meta.getChangedTargetStep() != null ) {
        data.changedRowSet = findOutputRowSet( meta.getChangedTargetStep().getName() );
        if ( data.changedRowSet == null ) {
          throw new KettleException(
            BaseMessages.getString( PKG, "CompareFields.Error.UnableToFindChangedOutputStep",
              meta.getChangedTargetStep().getName() ) );
        }
      }
      if ( meta.getAddedTargetStep() != null ) {
        data.addedRowSet = findOutputRowSet( meta.getAddedTargetStep().getName() );
        if ( data.addedRowSet == null ) {
          throw new KettleException(
            BaseMessages.getString( PKG, "CompareFields.Error.UnableToFindAddedOutputStep",
              meta.getAddedTargetStep().getName() ) );
        }
      }
      if ( meta.getRemovedTargetStep() != null ) {
        data.removedRowSet = findOutputRowSet( meta.getRemovedTargetStep().getName() );
        if ( data.removedRowSet == null ) {
          throw new KettleException(
            BaseMessages.getString( PKG, "CompareFields.Error.UnableToFindRemovedOutputStep",
              meta.getRemovedTargetStep().getName() ) );
        }
      }
    }

    CompareResult result = compareFields( getInputRowMeta(), row );

    RowSet outputRowSet = null;
    switch ( result.getType() ) {
      case IDENTICAL:
        if ( data.identicalRowSet != null ) {
          outputRowSet = data.identicalRowSet;
        }
        break;
      case CHANGED:
        if ( data.changedRowSet != null ) {
          outputRowSet = data.changedRowSet;
        }
        break;
      case ADDED:
        if ( data.addedRowSet != null ) {
          outputRowSet = data.addedRowSet;
        }
        break;
      case REMOVED:
        if ( data.removedRowSet != null ) {
          outputRowSet = data.removedRowSet;
        }
        break;
      default:
        break;
    }

    // Now that we have a result and a place to send the row, act upon it...
    //
    if ( outputRowSet != null ) {
      Object[] outputRow;
      if ( meta.isAddingFieldsList() ) {
        outputRow = RowDataUtil.addValueData( row, getInputRowMeta().size(), result.getChangedFieldNames() );
      } else {
        outputRow = row;
      }
      putRowTo( data.outputRowMeta, outputRow, outputRowSet );
    }

    return true;
  }

  private CompareResult compareFields( RowMetaInterface rowMeta, Object[] row ) throws KettleValueException {

    List<String> fieldsList = null;

    if ( meta.isAddingFieldsList() ) {
      fieldsList = new ArrayList<String>( meta.getCompareFields().size() );
    }

    List<CompareField> compareFields = meta.getCompareFields();

    boolean allIdentical = !compareFields.isEmpty();
    boolean allReferenceNull = !compareFields.isEmpty();
    boolean allCompareNull = !compareFields.isEmpty();
    boolean verifyAdded = !Const.isEmpty( meta.getAddedTargetStepname() );
    boolean verifyRemoved = !Const.isEmpty( meta.getRemovedTargetStepname() );

    // Compare all the fields
    //
    for ( CompareField field : compareFields ) {

      ValueMetaInterface referenceValueMeta = rowMeta.getValueMeta( field.getReferenceFieldIndex() );
      Object referenceValue = row[field.getReferenceFieldIndex()];

      ValueMetaInterface compareValueMeta = rowMeta.getValueMeta( field.getCompareFieldIndex() );
      Object compareValue = row[field.getCompareFieldIndex()];

      int result = referenceValueMeta.compare( referenceValue, compareValueMeta, compareValue );
      if ( result != 0 ) {
        allIdentical = false;
        if ( meta.isAddingFieldsList() ) {
          fieldsList.add( field.getReferenceFieldname() );
        }
      }
      if ( verifyAdded && !referenceValueMeta.isNull( referenceValue ) ) {
        allReferenceNull = false;
        // Stop checking
        verifyAdded = false;
      }
      if ( verifyRemoved && !compareValueMeta.isNull( compareValue ) ) {
        allCompareNull = false;
        // Stop checking
        verifyRemoved = false;
      }

    }

    // Evaluate what we found
    // 
    CompareResultType type;
    if ( allIdentical || ( verifyAdded && allReferenceNull && verifyRemoved && allCompareNull ) ) {
      type = CompareResultType.IDENTICAL;
    } else if ( allReferenceNull && data.addedRowSet != null ) {
      type = CompareResultType.ADDED;
    } else if ( allCompareNull && data.removedRowSet != null ) {
      type = CompareResultType.REMOVED;
    } else {
      type = CompareResultType.CHANGED;
    }

    String changedFieldNames = null;
    if ( !Const.isEmpty( fieldsList ) ) {
      StringBuilder b = new StringBuilder();
      for ( String fieldName : fieldsList ) {
        if ( b.length() > 0 ) {
          b.append( ", " );
        }
        b.append( fieldName );
      }
      changedFieldNames = b.toString();
    }

    return new CompareResult( type, changedFieldNames );
  }
}
