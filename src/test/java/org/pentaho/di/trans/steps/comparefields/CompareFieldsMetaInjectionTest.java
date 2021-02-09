package org.pentaho.di.trans.steps.comparefields;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.pentaho.di.core.injection.BaseMetadataInjectionTest;
import org.pentaho.di.junit.rules.RestorePDIEngineEnvironment;

public class CompareFieldsMetaInjectionTest  extends BaseMetadataInjectionTest<CompareFieldsMeta> {
    @ClassRule
    public static RestorePDIEngineEnvironment env = new RestorePDIEngineEnvironment();
    @Before
    public void setup() {
        setup( new CompareFieldsMeta() );
    }

    @Test
    public void test() throws Exception {
        check("ADD_FIELD_LIST", new BooleanGetter() {
            public boolean get() {
                return meta.isAddingFieldsList();
            }
        });
        check("FIELD_LIST_FIELD_NAME", new StringGetter() {
            public String get() {
                return meta.getFieldsListFieldname();
            }
        });
        check("REFERENCE_FIELD", new StringGetter() {
            public String get() {
                return meta.getCompareFields().get(0).getReferenceFieldname();
            }
        });
        check("COMPARE_FIELD", new StringGetter() {
            public String get() {
                return meta.getCompareFields().get(0).getCompareFieldname();
            }
        });
    }
}
