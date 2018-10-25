/*
 * Copyright 2017-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.rest.resources;

import io.atomix.primitive.partition.PartitionGroup;
import io.atomix.primitive.partition.PartitionService;
import io.atomix.protocols.raft.partition.RaftPartitionGroup;
import io.atomix.rest.AtomixResource;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.stream.Collectors;

/**
 * Management resource.
 */
@AtomixResource
@Path("/management")
public class ManagementResource {
  @GET
  @Path("/partition-groups")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getPartitionGroups(@Context PartitionService partitionService) {
    return Response.ok(partitionService.getPartitionGroups().stream()
        .map(PartitionGroup::config)
        .collect(Collectors.toList()))
        .build();
  }

  @GET
  @Path("/partition-groups/{group}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getPartitionGroup(@PathParam("group") String group, @Context PartitionService partitionService) {
    PartitionGroup partitionGroup = partitionService.getPartitionGroup(group);
    if (partitionGroup == null) {
      return Response.status(Response.Status.NOT_FOUND).build();
    }
    return Response.ok(partitionGroup.config()).build();
  }

  @POST
  @Path("/partition-groups/{group}/snapshot")
  public void snapshotPartitionGroup(@PathParam("group") String group, @Context PartitionService partitionService, @Suspended AsyncResponse response) {
    PartitionGroup partitionGroup = partitionService.getPartitionGroup(group);
    if (partitionGroup == null || partitionGroup.type() != RaftPartitionGroup.TYPE) {
      response.resume(Response.status(Response.Status.NOT_FOUND).build());
    } else {
      ((RaftPartitionGroup) partitionGroup).snapshot()
          .whenComplete((result, error) -> {
            if (error == null) {
              response.resume(Response.ok().build());
            } else {
              response.resume(Response.serverError().build());
            }
          });
    }
  }
}
