/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.spi.session;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class SessionConfigurationContext
{
    private final String user;
    private final Optional<String> source;
    private final Optional<String> catalog;
    private final Optional<String> schema;
    private final Set<String> clientTags;

    public SessionConfigurationContext(String user, Optional<String> source, Optional<String> catalog, Optional<String> schema, Set<String> clientTags)
    {
        this.user = requireNonNull(user, "user is null");
        this.source = requireNonNull(source, "source is null");
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.clientTags = new HashSet<>(requireNonNull(clientTags, "clientTags is null"));
    }

    public String getUser()
    {
        return user;
    }

    public Optional<String> getSource()
    {
        return source;
    }

    public Optional<String> getCatalog()
    {
        return catalog;
    }

    public Optional<String> getSchema()
    {
        return schema;
    }

    public Set<String> getClientTags()
    {
        return clientTags;
    }
}
