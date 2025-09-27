package framework.primarybackup;

import framework.atmostonce.AMOApplication;
import framework.atmostonce.AMOCommand;
import framework.atmostonce.AMOResult;
import framework.Address;
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

@Data
class Forward implements Message {
  private final Request request; // request to be executed
  private final Address client; // technically already part of request, but more explicit
}

@Data
class ForwardAck implements Message {
  private final Request request;
  private final Address client;
}