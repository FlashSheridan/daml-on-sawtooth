/*
 * Copyright 2019 Blockchain Technology Partners Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 * ------------------------------------------------------------------------------
 */
package com.blockchaintp.sawtooth.daml.rpc;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

import com.blockchaintp.sawtooth.daml.rpc.events.DamlLogEventHandler;
import com.blockchaintp.sawtooth.daml.util.Namespace;
import com.daml.ledger.participant.state.v1.TimeModel;
import com.daml.ledger.participant.state.v1.Configuration;
import com.daml.ledger.participant.state.v1.LedgerInitialConditions;
import com.daml.ledger.participant.state.v1.Offset;
import com.daml.ledger.participant.state.v1.ReadService;
import com.daml.ledger.participant.state.v1.Update;
import com.digitalasset.daml.lf.data.Time.Timestamp;
import com.digitalasset.ledger.api.health.HealthStatus;
import com.digitalasset.ledger.api.health.Healthy$;
import com.google.protobuf.ByteString;

import akka.NotUsed;
import akka.stream.scaladsl.Source;
import io.reactivex.Flowable;
import scala.Option;
import scala.Tuple2;

/**
 * Sawtooth implementation of the Daml ReadService.
 */
public class SawtoothReadService implements ReadService {

  private static final Logger LOGGER = Logger.getLogger(SawtoothReadService.class.getName());

  private static final int DEFAULT_MAX_TTL = 80; // 4x the TimeKeeper period

  private static final int DEFAULT_MAX_CLOCK_SKEW = 40; // 2x the TimeKeeper period

  private static final Timestamp BEGINNING_OF_EPOCH = new Timestamp(0);

  private static final long DEFAULT_MIN_TX_LATENCY = 1;

  private static final long DEFAULT_AVG_TX_LATENCY = 0;

  private static final long DEFAULT_MIN_SKEW = 30;

  private static final long DEFAULT_MAX_SKEW = 30;

  private final String url;
  private final ExecutorService executorService;
  private final SawtoothTransactionsTracer trace;
  private boolean startAtTheBeginning = false;

  private DamlLogEventHandler handler;

  /**
   * Build a ReadService based on a zmq address URL.
   *
   * @param zmqUrl the url of the zmq endpoint
   */
  public SawtoothReadService(final String zmqUrl) {
    this.url = zmqUrl;
    this.executorService = Executors.newWorkStealingPool();
    this.trace = null;
    this.handler = new DamlLogEventHandler(this.url);
    this.executorService.submit(this.handler);
  }

  /**
   * Build a ReadService based on a zmq address URL.
   *
   * @param zmqUrl     the url of the zmq endpoint
   * @param tracer     a transaction tracer
   * @param indexReset set to true if this reader should start at the first offset regardless of
   *                   subscription. This is useful in the case of the in memory reference index
   *                   server.
   */
  public SawtoothReadService(final String zmqUrl, final SawtoothTransactionsTracer tracer,
      final boolean indexReset) {
    this.url = zmqUrl;
    this.trace = tracer;
    this.executorService = Executors.newWorkStealingPool();
    this.handler = new DamlLogEventHandler(this.url);
    this.executorService.submit(this.handler);
    this.startAtTheBeginning = indexReset;
  }

  private TimeModel getDefaultTimeModel() {
    return TimeModel.apply(Duration.ofSeconds(DEFAULT_MIN_TX_LATENCY),
        Duration.ofSeconds(DEFAULT_MAX_CLOCK_SKEW), Duration.ofSeconds(DEFAULT_MAX_TTL),
        Duration.ofSeconds(DEFAULT_AVG_TX_LATENCY), Duration.ofSeconds(DEFAULT_MIN_SKEW),
        Duration.ofSeconds(DEFAULT_MAX_SKEW)).get();
  }

  @Override
  public final Source<LedgerInitialConditions, NotUsed> getLedgerInitialConditions() {
    TimeModel tm = getDefaultTimeModel();
    LOGGER.info(String.format("TimeModel set to %s", tm));

    String ledgerId = "default-ledgerid";
    ByteString data = this.handler.getState(Namespace.DAML_CONFIG_LEDGER_ID);
    if (data != null) {
      ledgerId = data.toStringUtf8();
    }
    Configuration blankConfiguration = new Configuration(0, tm);
    Flowable<LedgerInitialConditions> f = Flowable.fromArray(new LedgerInitialConditions[] {
        new LedgerInitialConditions(ledgerId, blankConfiguration, BEGINNING_OF_EPOCH)});
    return Source.fromPublisher(f);
  }

  @Override
  public final Source<Tuple2<Offset, Update>, NotUsed> stateUpdates(
      final Option<Offset> beginAfter) {
    if (beginAfter.isDefined()) {
      LOGGER.info(String.format("Starting event handling at offset=%s", beginAfter.get()));
      this.handler.sendSubscribe(beginAfter.get());
    } else {
      if (this.startAtTheBeginning) {
        LOGGER.info("Starting at the beginning of the chain (offset=1-0) as requested");
        Offset offset = new Offset(new long[] {1, 0});
        this.handler.sendSubscribe(offset);
      } else {
        LOGGER.info(String.format("Starting event handling at wherever is current"));
        this.handler.sendSubscribe();
      }
    }
    if (this.trace != null) {
      this.handler.setTracer(this.trace);
    }
    return Source.fromPublisher(this.handler.getPublisher());
  }

  @Override
  public final HealthStatus currentHealth() {
    return Healthy$.MODULE$;
  }
}
