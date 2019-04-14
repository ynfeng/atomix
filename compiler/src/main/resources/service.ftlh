/*
 * Copyright 2019-present Open Networking Foundation
 *
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
package ${serviceClass.javaPackage};

/**
 * Atomix generated service interface ${serviceClass.className}.
 */
public abstract class ${serviceClass.className} extends io.atomix.primitive.service.AbstractPrimitiveService {
    protected ${serviceClass.className}(io.atomix.primitive.PrimitiveType primitiveType) {
        super(primitiveType);
    }

    @Override
    protected void configure(io.atomix.primitive.service.ServiceExecutor executor) {
        <#list operations as operation>
        executor.register(
            ${operation.operationsClass.qualifiedName}.${operation.enumValue},
            this::${operation.methodName},
            ${operation.requestClass.qualifiedName}::parseFrom,
            ${operation.responseClass.qualifiedName}::toByteArray);
         </#list>
    }

    <#list operations as operation>
    /**
     * Atomix generated method ${operation.methodName}.
     *
     * @param request the request object
     * @return the response object
     */
    public abstract ${operation.responseClass.qualifiedName} ${operation.methodName}(${operation.requestClass.qualifiedName} request);

    </#list>
}