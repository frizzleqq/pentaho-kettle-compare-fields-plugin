package org.apache.hop.pipeline.transforms.comparefields;

/**
 * 
 */

import org.apache.hop.core.IRowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.errorhandling.IStream;
import org.apache.hop.pipeline.transforms.comparefields.CompareResult.CompareResultType;

import java.util.ArrayList;
import java.util.List;

/**
 * Compare a list of 2 field each with each other.
 *  
 * @author matt
 *
 */
public class CompareFields extends BaseTransform<CompareFieldsMeta, CompareFieldsData> implements ITransform<CompareFieldsMeta, CompareFieldsData> {

  private static Class<?> PKG = CompareFields.class; // for i18n purposes, needed by Translator2!!

  public CompareFields(TransformMeta transformMeta, CompareFieldsMeta meta, CompareFieldsData data, int copyNr, PipelineMeta pipelineMeta,
                       Pipeline pipeline ) {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );
  }

  @Override
  public boolean processRow() throws HopException {

    // Get one row from previous step(s).
    Object[] row = getRow();

    if ( row == null ) {
      setOutputDone();
      return false;
    }

    if ( first ) {
      first = false;

      // determine the output fields
      data.outputRowMeta = getInputRowMeta().clone();
      meta.getFields( getInputRowMeta(), getTransformName(), null, null, this, metadataProvider );

      if ( meta.getCompareFields().isEmpty() ) {
        throw new HopException( BaseMessages.getString( PKG, "CompareFields.Error.NoFieldsToCompare" ) );
      }

      // Index the compare fields
      for ( CompareField field : meta.getCompareFields() ) {
        field.index( getInputRowMeta() );
      }

      List<IStream> targetStreams = meta.getTransformIOMeta().getTargetStreams();

      if ( meta.getIdenticalTargetTransformMeta() != null ) {
        data.identicalRowSet = findOutputRowSet(meta.getIdenticalTargetTransformMeta().getName()  );
        if ( data.identicalRowSet == null ) {
          throw new HopException(
            BaseMessages.getString( PKG, "CompareFields.Error.UnableToFindIdenticalOutputStep",
              meta.getIdenticalTargetTransformMeta().getName() ) );
        }
      }
      if ( meta.getChangedTargetTransformMeta() != null ) {
        data.changedRowSet = findOutputRowSet( meta.getChangedTargetTransformMeta().getName() );
        if ( data.changedRowSet == null ) {
          throw new HopException(
            BaseMessages.getString( PKG, "CompareFields.Error.UnableToFindChangedOutputStep",
              meta.getChangedTargetTransformMeta().getName() ) );
        }
      }
      if ( meta.getAddedTargetTransformMeta() != null ) {
        data.addedRowSet = findOutputRowSet( meta.getAddedTargetTransformMeta().getName() );
        if ( data.addedRowSet == null ) {
          throw new HopException(
            BaseMessages.getString( PKG, "CompareFields.Error.UnableToFindAddedOutputStep",
              meta.getAddedTargetTransformMeta().getName() ) );
        }
      }
      if ( meta.getRemovedTargetTransformMeta() != null ) {
        data.removedRowSet = findOutputRowSet( meta.getRemovedTargetTransformMeta().getName() );
        if ( data.removedRowSet == null ) {
          throw new HopException(
            BaseMessages.getString( PKG, "CompareFields.Error.UnableToFindRemovedOutputStep",
              meta.getRemovedTargetTransformMeta().getName() ) );
        }
      }
    }

    CompareResult result = compareFields( getInputRowMeta(), row );

    IRowSet outputRowSet = null;
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

    // Now that we have a result and a place to send the row, act upon it
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

  private CompareResult compareFields(IRowMeta rowMeta, Object[] row ) throws HopValueException {

    List<String> fieldsList = null;

    if ( meta.isAddingFieldsList() ) {
      fieldsList = new ArrayList<String>( meta.getCompareFields().size() );
    }

    List<CompareField> compareFields = meta.getCompareFields();

    boolean allIdentical = !compareFields.isEmpty();
    boolean allReferenceNull = !compareFields.isEmpty();
    boolean allCompareNull = !compareFields.isEmpty();
    boolean verifyAdded = !Utils.isEmpty( meta.getAddedTargetStepname() );
    boolean verifyRemoved = !Utils.isEmpty( meta.getRemovedTargetStepname() );

    // Compare all the fields
    //
    for ( CompareField field : compareFields ) {

      IValueMeta referenceValueMeta = rowMeta.getValueMeta( field.getReferenceFieldIndex() );
      Object referenceValue = row[field.getReferenceFieldIndex()];

      IValueMeta compareValueMeta = rowMeta.getValueMeta( field.getCompareFieldIndex() );
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
    if ( !Utils.isEmpty( fieldsList ) ) {
      StringBuilder b = new StringBuilder();
      for ( String fieldName : fieldsList ) {
        if ( b.length() > 0 ) {
          b.append( "," );
        }
        b.append( fieldName );
      }
      changedFieldNames = b.toString();
    }

    return new CompareResult( type, changedFieldNames );
  }
}
