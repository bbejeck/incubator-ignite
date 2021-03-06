/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.resources;

import java.lang.annotation.*;

/**
 * Annotates a field or a setter method for any custom resources injection.
 * It can be injected into grid tasks and grid jobs. Use it when you would
 * like, for example, to inject something like JDBC connection pool into tasks
 * or jobs - this way your connection pool will be instantiated only once
 * per task and reused for all executions of this task.
 * <p>
 * You can inject other resources into your user resource.
 * The following grid resources can be injected:
 * <ul>
 * <li>{@link IgniteLoggerResource}</li>
 * <li>{@link IgniteLocalNodeIdResource}</li>
 * <li>{@link IgniteHomeResource}</li>
 * <li>{@link IgniteNameResource}</li>
 * <li>{@link IgniteMBeanServerResource}</li>
 * <li>{@link IgniteExecutorServiceResource}</li>
 * <li>{@link IgniteMarshallerResource}</li>
 * <li>{@link IgniteSpringApplicationContextResource}</li>
 * <li>{@link IgniteSpringResource}</li>
 * <li>{@link IgniteInstanceResource}</li>
 * </ul>
 * Refer to corresponding resource documentation for more information.
 * <p>
 * <h1 class="header">Resource Class</h1>
 * The resource will be created based on the {@link #resourceClass()} value. If
 * If {@code resourceClass} is not specified, then field type or setter parameter
 * type will be used to infer the class type of the resource. Set {@link #resourceClass()}
 * to a specific value if the class of resource cannot be inferred from field or setter
 * declaration (for example, if field is an interface).
 * <p>
 * <h1 class="header">Resource Life Cycle</h1>
 * User resource will be instantiated once on every node where task is deployed.
 * Basically there will always be only one instance of resource on any
 * grid node for any task class. Every node will instantiate
 * it's own copy of user resources used for every deployed task (see
 * {@link IgniteUserResourceOnDeployed} and {@link IgniteUserResourceOnUndeployed}
 * annotation for resource deployment and undeployment callbacks). For this
 * reason <b>resources should not be sent to remote nodes and should
 * always be declared as transient</b> just in case.
 * <p>
 * Note that an instance of user resource will be created for every deployed task.
 * In case if you need a singleton resource instances on grid nodes (not per-task),
 * you can use {@link IgniteSpringApplicationContextResource} for injecting per-VM
 * singleton resources configured in Spring.
 * <p>
 * <h1 class="header">Examples</h1>
 * Here is how injection would typically happen:
 * <pre name="code" class="java">
 * public class MyGridJob implements ComputeJob {
 *      ...
 *      &#64;GridUserResource
 *      private transient MyUserResource rsrc;
 *      ...
 *  }
 * </pre>
 * or
 * <pre name="code" class="java">
 * public class MyGridJob implements ComputeJob {
 *     ...
 *     private transient MyUserResource rsrc;
 *     ...
 *     &#64;GridUserResource
 *     public void setMyUserResource(MyUserResource rsrc) {
 *          this.rsrc = rsrc;
 *     }
 *     ...
 * }
 * </pre>
 * where resource class can look like this:
 * <pre name="code" class="java">
 * public class MyUserResource {
 *     ...
 *     // Inject logger (or any other resource).
 *     &#64;GridLoggerResource
 *     private GridLogger log;
 *
 *     // Inject grid instance (or any other resource).
 *     &#64;GridInstanceResource
 *     private Grid grid;
 *
 *     // Deployment callback.
 *     &#64;GridUserResourceOnDeployed
 *     public void deploy() {
 *        // Some initialization logic.
 *        ...
 *     }
 *
 *     // Undeployment callback.
 *     &#64;GridUserResourceOnUndeployed
 *     public void undeploy() {
 *        // Some clean up logic.
 *        ...
 *     }
 * }
 * </pre>
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.FIELD})
public @interface IgniteUserResource {
    /**
     * Optional resource class. By default the type of the resource variable
     * or setter parameter will be used.
     */
    @SuppressWarnings({"JavaDoc"}) Class<?> resourceClass() default Void.class;

    /**
     * Optional resource name. By default the {@code "dfltName"} values will be used.
     */
    @SuppressWarnings({"JavaDoc"}) String resourceName() default "dfltName";
}
