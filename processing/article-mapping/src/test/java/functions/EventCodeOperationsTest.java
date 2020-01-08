package functions;

import models.EventCode;
import org.junit.Assert;
import org.junit.Test;

public class EventCodeOperationsTest {

    @Test
    public void mapEventCodeToEventCode() throws Exception {

        String s = "1823\t" +
                "Kill by physical assault";

        EventCode eventCode = new EventCodeOperations.MapEventCodeToEventCode().call(s);
        Assert.assertEquals("1823", eventCode.getCameoEventCode());
        Assert.assertEquals("Kill by physical assault", eventCode.getEventDescription());
    }
}
