package framework.primarybackup;

import atmostonce.AMOApplication;
import atmostonce.AMOCommand;
import atmostonce.AMOResult;
import framework.Application;
import framework.Message;
import lombok.Data;

/* -----------------------------------------------------------------------------------------------
 *  ViewServer Messages
 * ---------------------------------------------------------------------------------------------*/
@Data
class Ping implements Message {
  private final int viewNum;
}

@Data
class GetView implements Message {}

@Data
class ViewReply implements Message {
  private final View view;
}

/* -----------------------------------------------------------------------------------------------
 *  Primary-Backup Messages
 * ---------------------------------------------------------------------------------------------*/
@Data
class Request implements Message {
  private final AMOCommand command;
  private final View view;
}

@Data
class Reply implements Message {
  private final AMOResult result;
  private final View view;
}

@Data
class StateTransfer implements Message {
  private final AMOApplication<Application> amoApplication;
  private final View view;
}

@Data
class StateTransferAck implements Message {
  private final View view;
}
