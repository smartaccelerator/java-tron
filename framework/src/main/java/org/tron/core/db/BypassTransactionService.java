package org.tron.core.db;

import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.tron.core.capsule.TransactionCapsule;
import org.tron.core.exception.BadItemException;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

@Slf4j(topic = "DB")
public class BypassTransactionService {

  private static final int DEFAULT_BIND_PORT = 95555;
  private static final int DEFAULT_QUEUE_LENGTH = 10000;
  private static BypassTransactionService instance;
  private ZContext context = null;
  private ZMQ.Socket dealer = null;
  private ScheduledExecutorService fetchExecutor = Executors.newSingleThreadScheduledExecutor();

  @Autowired
  Manager manager;

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

  public boolean start() {
    context = new ZContext(1);
    dealer = context.createSocket(SocketType.DEALER);

    if (Objects.isNull(dealer)) {
      return false;
    }

    int bindPort = DEFAULT_BIND_PORT;
    int sendQueueLength = DEFAULT_QUEUE_LENGTH;

    context.setSndHWM(sendQueueLength);

    String bindAddress = String.format("tcp://*:%d", bindPort);
    if (!dealer.bind(bindAddress)) {
      return false;
    }

    fetchExecutor.schedule(() -> {
      fetchTransaction();
    }, 1, TimeUnit.MILLISECONDS);

    logger.info("start bypass TransactionService");
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
        TransactionCapsule transactionCapsule = new TransactionCapsule(dealer.recv(0));
        manager.pushTransaction(transactionCapsule);
      } catch (BadItemException e) {
        logger.error("parse recv transaction failed, error={}", e.getMessage());
      } catch (Exception e) {
        logger.error("push transaction failed, error={}", e.getMessage());
      }
    }
  }
}
