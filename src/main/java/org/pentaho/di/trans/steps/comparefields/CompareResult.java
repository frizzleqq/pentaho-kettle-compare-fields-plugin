package org.pentaho.di.trans.steps.comparefields;


public class CompareResult {

  public enum CompareResultType {
    IDENTICAL, CHANGED, ADDED, REMOVED,
  }

  private CompareResultType type;
  private String changedFieldNames;

  public CompareResult( CompareResultType type, String changedFieldNames ) {
    super();
    this.type = type;
    this.changedFieldNames = changedFieldNames;
  }

  public CompareResultType getType() {
    return type;
  }

  public void setType( CompareResultType type ) {
    this.type = type;
  }

  public String getChangedFieldNames() {
    return changedFieldNames;
  }

  public void setChangedFieldNames( String changedFieldNames ) {
    this.changedFieldNames = changedFieldNames;
  }

}
