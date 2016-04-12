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

package org.apache.ignite.lang;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.CacheEntryListener;
import org.apache.ignite.configuration.IgniteConfiguration;

/**
 * If callback has this annotation when it will be executing in another thread.
 * <p/>
 * For example, if {@link CacheEntryEventFilter filter} or {@link CacheEntryListener}
 * annotated this annotation then they will be executing on a separate thread pool. It allows
 * to use cache API in a callbacks.
 * <p/>
 * Different implementations can use different thread pools. For example continuous query will use continuous query
 * thread poll which can be configured by {@link IgniteConfiguration#setAsyncCallbackPoolSize(int)}
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface IgniteAsyncCallback {
    // No-op.
}
