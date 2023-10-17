package org.tron.core.net;

import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.tron.core.capsule.TransactionCapsule;
import org.tron.core.exception.BadItemException;
import org.tron.core.net.message.adv.TransactionMessage;
import org.tron.core.net.messagehandler.TransactionsMsgHandler;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

@Slf4j(topic = "DB")
public class BypassTransactionService {

  private static final int DEFAULT_BIND_PORT = 15555;
  private static final int DEFAULT_QUEUE_LENGTH = 10000;
  private static BypassTransactionService instance;
  private ZContext context = null;
  private ZMQ.Socket dealer = null;
  private ScheduledExecutorService fetchExecutor = Executors.newSingleThreadScheduledExecutor();

  private TransactionsMsgHandler transactionMsgHandler = null;

  public static BypassTransactionService getInstance() {
    if (Objects.isNull(instance)) {
      synchronized (BypassTransactionService.class) {
        if (Objects.isNull(instance)) {
          instance = new BypassTransactionService();
        }
      }
    }
    return instance;
  }

  public boolean start(TransactionsMsgHandler transactionMsgHandler) {
    this.transactionMsgHandler = transactionMsgHandler;
    context = new ZContext(1);
    dealer = context.createSocket(SocketType.DEALER);

    if (Objects.isNull(dealer)) {
      return false;
    }

    int bindPort = DEFAULT_BIND_PORT;
    int sendQueueLength = DEFAULT_QUEUE_LENGTH;

    context.setSndHWM(sendQueueLength);

    String bindAddress = String.format("tcp://127.0.0.1:%d", bindPort);
    if (!dealer.bind(bindAddress)) {
      return false;
    }

    fetchExecutor.schedule(() -> {
      fetchTransaction();
    }, 1, TimeUnit.MILLISECONDS);

    logger.info("txpool:start bypass TransactionService");
    return true;
  }

  public void stop() {
    if (Objects.nonNull(dealer)) {
      dealer.close();
    }

    if (Objects.nonNull(context)) {
      context.close();
    }
  }

  protected void fetchTransaction() {
    while (true) {
      try {
        TransactionMessage transactionMessage = new TransactionMessage(dealer.recv(0));
        transactionMsgHandler.processMessage(transactionMessage);
      } catch (BadItemException e) {
        logger.error("txpool: parse recv transaction failed, error={}", e.getMessage());
      } catch (Exception e) {
        logger.error("txpool: push transaction failed, error={}", e.getMessage());
      }
    }
  }
}
