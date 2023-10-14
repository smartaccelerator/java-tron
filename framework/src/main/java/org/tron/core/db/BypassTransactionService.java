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

  private static final int DEFAULT_BIND_PORT = 15555;
  private static final int DEFAULT_QUEUE_LENGTH = 10000;
  private static BypassTransactionService instance;
  private ZContext context = null;
  private ZMQ.Socket dealer = null;
  private ScheduledExecutorService fetchExecutor = Executors.newSingleThreadScheduledExecutor();

  private Manager manager = null;

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

  public boolean start(Manager dbManager) {
    this.manager = dbManager;
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
    long success = 0;
    long fail = 0;

    while (true) {
      try {
        TransactionCapsule transactionCapsule = new TransactionCapsule(dealer.recv(0));
        if (manager != null) {
          manager.pushTransaction(transactionCapsule);
        } else {
          logger.error("txpool: manager is null");
        }
        success += 1;
        if (success % 100 == 0) {
          logger.info("txpool: success={} fail={}", success, fail);
        }
        // logger.info("txpool: pushTransaction success");
      } catch (BadItemException e) {
        fail += 1;
        // logger.error("txpool: parse recv transaction failed, error={}", e.getMessage());
      } catch (Exception e) {
        fail += 1;
        // logger.error("txpool: push transaction failed, error={}", e.getMessage());
      }
    }
  }
}
