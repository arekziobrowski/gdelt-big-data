package models;

import java.io.Serializable;
import java.util.Objects;

public class EventCode implements Serializable {

    private String cameoEventCode;
    private String eventDescription;

    public EventCode(String cameoEventCode, String eventDescription) {
        this.cameoEventCode = cameoEventCode;
        this.eventDescription = eventDescription;
    }

    @Override
    public String toString() {
        return Objects.toString(cameoEventCode, "") + '\t'
                + Objects.toString(eventDescription, "");
    }

    public String getCameoEventCode() {
        return cameoEventCode;
    }

    public void setCameoEventCode(String cameoEventCode) {
        this.cameoEventCode = cameoEventCode;
    }

    public String getEventDescription() {
        return eventDescription;
    }

    public void setEventDescription(String eventDescription) {
        this.eventDescription = eventDescription;
    }
}
