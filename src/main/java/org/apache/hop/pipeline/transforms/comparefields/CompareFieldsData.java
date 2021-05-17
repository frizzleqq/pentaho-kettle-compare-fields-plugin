package org.apache.hop.pipeline.transforms.comparefields;

/**
 * 
 */


import org.apache.hop.core.IRowSet;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

/**
 * @author matt
 *
 */
public class CompareFieldsData extends BaseTransformData implements ITransformData {

  public IRowMeta outputRowMeta;
  public IRowSet identicalRowSet = null;
  public IRowSet changedRowSet = null;
  public IRowSet addedRowSet = null;
  public IRowSet removedRowSet = null;

  public CompareFieldsData() {
    super();
  }

}
