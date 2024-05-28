package com.mint.db.raft;

import com.mint.db.Raft;
import com.mint.db.config.NodeConfig;
import com.mint.db.grpc.InternalGrpcActor;
import com.mint.db.replication.model.PersistentState;
import org.awaitility.Awaitility;
import org.hamcrest.comparator.ComparatorMatcherBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.mint.db.util.LogUtil.protobufMessageToString;

@RunWith(PowerMockRunner.class)
class RaftActorTest {
    private static final Logger logger = LoggerFactory.getLogger(RaftActorTest.class);

    @Test
    void leaderElectionTest() throws IOException {
        Path logDir = Files.createTempDirectory("mintKVLogDir");
        NodeConfig config = new NodeConfig(
                8080,
                0,
                logDir.toString(),
                List.of(
                        "http://localhost:8080",
                        "http://localhost:8081",
                        "http://localhost:8082",
                        "http://localhost:8083",
                        "http://localhost:8084"
                )
        );

        List<RaftActor> cluster = new ArrayList<>(5);

        InternalGrpcActor internalGrpcActor = Mockito.mock(InternalGrpcActor.class);
        AtomicInteger internalGrpcActorInvocations = new AtomicInteger();
        Mockito.doAnswer(invocation -> {
            Raft.VoteRequest voteRequest = invocation.getArgument(0);
            int nodeId = voteRequest.getCandidateId();
            logger.info("internalGrpcActor.onLeaderCandidate({})", protobufMessageToString(voteRequest));
            for (int i = 0; i < cluster.size(); i++) {
                if (i != nodeId) {
                    var voteResponse = cluster.get(i).onRequestVote(voteRequest);
                    if (nodeId == 0) {
                        Assertions.assertTrue(voteResponse.getVoteGranted());
                        Assertions.assertEquals(1, voteResponse.getTerm());
                        cluster.get(nodeId).onVoteResponse(voteResponse);
                    } else {
                        Assertions.assertFalse(voteResponse.getVoteGranted());
                    }
                }
            }
            internalGrpcActorInvocations.incrementAndGet();
            return null;
        }).when(internalGrpcActor).onLeaderCandidate(Mockito.any(Raft.VoteRequest.class));

        for (int i = 0; i < 5; i++) {
            final NodeConfig nodeConfig = config.copy();
            nodeConfig.setNodeId(i);
            nodeConfig.setPort(8080 + i);

            Random random = Mockito.mock(Random.class);
            Mockito.when(random.nextLong(1000L, 5000L)).thenReturn((i + 1) * 5000L);
            Whitebox.setInternalState(RaftActor.class, "rand", random);

            RaftActor raftActor = new RaftActor(internalGrpcActor, nodeConfig, new PersistentState());
            cluster.add(raftActor);
        }
        //CHECKSTYLE.OFF: IndentationCheck
        Awaitility.await().atMost(11, TimeUnit.SECONDS)
                .untilAtomic(
                        internalGrpcActorInvocations,
                        ComparatorMatcherBuilder.comparedBy(Integer::compareTo).greaterThan(0)
                );
        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .failFast(() -> internalGrpcActorInvocations.get() > 1)
                .untilAtomic(
                        internalGrpcActorInvocations,
                        ComparatorMatcherBuilder.comparedBy(Integer::compareTo).comparesEqualTo(1)
                );
        Mockito.verify(internalGrpcActor, Mockito.times(1))
                .onAppendEntityRequest(Mockito.any(Raft.AppendEntriesRequest.class));
        //CHECKSTYLE.ON: IndentationCheck
    }
}
