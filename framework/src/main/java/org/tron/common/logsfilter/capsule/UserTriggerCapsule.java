package org.tron.common.logsfilter.capsule;

import lombok.Getter;
import lombok.Setter;
import org.tron.common.logsfilter.EventPluginLoader;
import org.tron.common.logsfilter.trigger.SolidityTrigger;

public class UserTriggerCapsule extends TriggerCapsule {

  @Getter
  @Setter
  private String topic;

  public UserTriggerCapsule(String topic) {
    this.topic = topic;
  }

  @Override
  public void processTrigger() {
    EventPluginLoader.getInstance().commitUserTrigger(this.topic);
  }
}

