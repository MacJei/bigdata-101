import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Aliaksei_Neuski on 9/20/16.
 */
@UDFType(deterministic = false)
@Description(
        name="agent_pars_udf",
        value="Parse user-agent string into separate fields. v.1.5.",
        extended="Adds UA_TYPE, UA_FAMILY, OS_NAME, DEVICE based on user-agent."
)
public class UserAgentUDTF5 extends GenericUDTF {

    private static final String UA_TYPE = "UA_TYPE";
    private static final String UA_FAMILY = "UA_FAMILY";
    private static final String OS_NAME = "OS_NAME";
    private static final String DEVICE = "DEVICE";

    private PrimitiveObjectInspector stringOI = null;

    public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
        if (args.length != 1) {
            throw new UDFArgumentException("UserAgentUDTF5() takes exactly one argument!");
        }

        if (args[0].getCategory() != ObjectInspector.Category.PRIMITIVE
                && ((PrimitiveObjectInspector) args[0]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            throw new UDFArgumentException("NameParserGenericUDTF() takes a string as a parameter!");
        }

        // input inspectors
        stringOI = (PrimitiveObjectInspector) args[0];

        // output inspectors -- an object with four fields
        List<String> fieldNames = new ArrayList<>(4);
        List<ObjectInspector> fieldOIs = new ArrayList<>(4);

        fieldNames.add(UA_TYPE);
        fieldOIs.add(PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.STRING));

        fieldNames.add(UA_FAMILY);
        fieldOIs.add(PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.STRING));

        fieldNames.add(OS_NAME);
        fieldOIs.add(PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.STRING));

        fieldNames.add(DEVICE);
        fieldOIs.add(PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.STRING));

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    private List<Object> processInputRecord(String name) {
        // forwarded collection
        List<Object> fwdCollection = new ArrayList<>();

        // ignoring null or empty input
        if (name == null || name.isEmpty()) {
            return fwdCollection;
        }

        UserAgent ua = UserAgent.parseUserAgentString(name);

        // UA type
        fwdCollection.add(ua.getBrowser().getBrowserType().getName());
        // UA family
        fwdCollection.add(ua.getBrowser().getGroup().getName());
        // OS name
        fwdCollection.add(ua.getOperatingSystem().getName());
        // Device
        fwdCollection.add(ua.getOperatingSystem().getDeviceType().getName());

        return fwdCollection;
    }

    @Override
    public void process(Object[] arg) throws HiveException {
        final String name = stringOI.getPrimitiveJavaObject(arg[0]).toString();

        List<Object> fwdCollection = processInputRecord(name);

        forward(fwdCollection.toArray());
    }

    @Override
    public void close() throws HiveException {
        // do nothing
    }
}