// Copyright 2018 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.vespa.athenz.client.zms;

import com.yahoo.vespa.athenz.api.AthenzDomain;
import com.yahoo.vespa.athenz.api.AthenzIdentity;
import com.yahoo.vespa.athenz.api.AthenzResourceName;
import com.yahoo.vespa.athenz.api.AthenzRole;
import com.yahoo.vespa.athenz.api.AthenzUser;
import com.yahoo.vespa.athenz.api.OktaAccessToken;
import com.yahoo.vespa.athenz.api.OktaIdentityToken;
import com.yahoo.vespa.athenz.client.ErrorHandler;
import com.yahoo.vespa.athenz.client.common.ClientBase;
import com.yahoo.vespa.athenz.client.zms.bindings.AccessResponseEntity;
import com.yahoo.vespa.athenz.client.zms.bindings.AssertionEntity;
import com.yahoo.vespa.athenz.client.zms.bindings.DomainListResponseEntity;
import com.yahoo.vespa.athenz.client.zms.bindings.MembershipEntity;
import com.yahoo.vespa.athenz.client.zms.bindings.PolicyEntity;
import com.yahoo.vespa.athenz.client.zms.bindings.ProviderResourceGroupRolesRequestEntity;
import com.yahoo.vespa.athenz.client.zms.bindings.RoleEntity;
import com.yahoo.vespa.athenz.client.zms.bindings.TenancyRequestEntity;
import com.yahoo.vespa.athenz.identity.ServiceIdentityProvider;
import com.yahoo.vespa.athenz.utils.AthenzIdentities;
import org.apache.http.Header;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHeader;

import javax.net.ssl.SSLContext;
import java.net.URI;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

/**
 * @author bjorncs
 */
public class DefaultZmsClient extends ClientBase implements ZmsClient {

    private final URI zmsUrl;
    private final AthenzIdentity identity;

    public DefaultZmsClient(URI zmsUrl, AthenzIdentity identity, SSLContext sslContext, ErrorHandler errorHandler) {
        this(zmsUrl, identity, () -> sslContext, errorHandler);
    }

    public DefaultZmsClient(URI zmsUrl, ServiceIdentityProvider identityProvider, ErrorHandler errorHandler) {
        this(zmsUrl, identityProvider.identity(), identityProvider::getIdentitySslContext, errorHandler);
    }

    private DefaultZmsClient(URI zmsUrl, AthenzIdentity identity, Supplier<SSLContext> sslContextSupplier, ErrorHandler errorHandler) {
        super("vespa-zms-client", sslContextSupplier, ZmsClientException::new, null, errorHandler);
        this.zmsUrl = addTrailingSlash(zmsUrl);
        this.identity = identity;
    }

    private static URI addTrailingSlash(URI zmsUrl) {
        return zmsUrl.getPath().endsWith("/") ? zmsUrl : URI.create(zmsUrl.toString() + '/');
    }

    @Override
    public void createTenancy(AthenzDomain tenantDomain, AthenzIdentity providerService, OktaIdentityToken identityToken, OktaAccessToken accessToken) {
        URI uri = zmsUrl.resolve(String.format("domain/%s/tenancy/%s", tenantDomain.getName(), providerService.getFullName()));
        HttpUriRequest request = RequestBuilder.put()
                .setUri(uri)
                .addHeader(createCookieHeaderWithOktaTokens(identityToken, accessToken))
                .setEntity(toJsonStringEntity(new TenancyRequestEntity(tenantDomain, providerService, Collections.emptyList())))
                .build();
        execute(request, response -> readEntity(response, Void.class));
    }

    @Override
    public void deleteTenancy(AthenzDomain tenantDomain, AthenzIdentity providerService, OktaIdentityToken identityToken, OktaAccessToken accessToken) {
        URI uri = zmsUrl.resolve(String.format("domain/%s/tenancy/%s", tenantDomain.getName(), providerService.getFullName()));
        HttpUriRequest request = RequestBuilder.delete()
                .setUri(uri)
                .addHeader(createCookieHeaderWithOktaTokens(identityToken, accessToken))
                .build();
        execute(request, response -> readEntity(response, Void.class));
    }

    @Override
    public void createProviderResourceGroup(AthenzDomain tenantDomain, AthenzIdentity providerService, String resourceGroup,
                                            Set<RoleAction> roleActions, OktaIdentityToken identityToken, OktaAccessToken accessToken) {
        URI uri = zmsUrl.resolve(String.format("domain/%s/provDomain/%s/provService/%s/resourceGroup/%s", tenantDomain.getName(), providerService.getDomainName(), providerService.getName(), resourceGroup));
        HttpUriRequest request = RequestBuilder.put()
                .setUri(uri)
                .addHeader(createCookieHeaderWithOktaTokens(identityToken, accessToken))
                .setEntity(toJsonStringEntity(new ProviderResourceGroupRolesRequestEntity(providerService, tenantDomain, roleActions, resourceGroup)))
                .build();
        execute(request, response -> readEntity(response, Void.class)); // Note: The ZMS API will actually return a json object that is similar to ProviderResourceGroupRolesRequestEntity
    }

    @Override
    public void deleteProviderResourceGroup(AthenzDomain tenantDomain, AthenzIdentity providerService, String resourceGroup,
                                            OktaIdentityToken identityToken, OktaAccessToken accessToken) {
        URI uri = zmsUrl.resolve(String.format("domain/%s/provDomain/%s/provService/%s/resourceGroup/%s", tenantDomain.getName(), providerService.getDomainName(), providerService.getName(), resourceGroup));
        HttpUriRequest request = RequestBuilder.delete()
                .setUri(uri)
                .addHeader(createCookieHeaderWithOktaTokens(identityToken, accessToken))
                .build();
        execute(request, response -> readEntity(response, Void.class));
    }

    @Override
    public void addRoleMember(AthenzRole role, AthenzIdentity member) {
        URI uri = zmsUrl.resolve(String.format("domain/%s/role/%s/member/%s", role.domain().getName(), role.roleName(), member.getFullName()));
        MembershipEntity membership = new MembershipEntity(member.getFullName(), true, role.roleName(), null);
        HttpUriRequest request = RequestBuilder.put(uri)
                .setEntity(toJsonStringEntity(membership))
                .build();
        execute(request, response -> readEntity(response, Void.class));
    }

    @Override
    public void deleteRoleMember(AthenzRole role, AthenzIdentity member) {
        URI uri = zmsUrl.resolve(String.format("domain/%s/role/%s/member/%s", role.domain().getName(), role.roleName(), member.getFullName()));
        HttpUriRequest request = RequestBuilder.delete(uri).build();
        execute(request, response -> readEntity(response, Void.class));
    }

    @Override
    public boolean getMembership(AthenzRole role, AthenzIdentity identity) {
        URI uri = zmsUrl.resolve(String.format("domain/%s/role/%s/member/%s", role.domain().getName(), role.roleName(), identity.getFullName()));
        HttpUriRequest request = RequestBuilder.get()
                .setUri(uri)
                .build();
        return execute(request, response -> {
            MembershipEntity membership = readEntity(response, MembershipEntity.class);
            return membership.isMember;
        });
    }

    @Override
    public List<AthenzDomain> getDomainList(String prefix) {
        HttpUriRequest request = RequestBuilder.get()
                .setUri(zmsUrl.resolve("domain"))
                .addParameter("prefix", prefix)
                .build();
        return execute(request, response -> {
            DomainListResponseEntity result = readEntity(response, DomainListResponseEntity.class);
            return result.domains.stream().map(AthenzDomain::new).collect(toList());
        });
    }

    @Override
    public boolean hasAccess(AthenzResourceName resource, String action, AthenzIdentity identity) {
        URI uri = zmsUrl.resolve(String.format("access/%s/%s?principal=%s",
                                               action, resource.toResourceNameString(), identity.getFullName()));
        HttpUriRequest request = RequestBuilder.get()
                .setUri(uri)
                .build();
        return execute(request, response -> {
            AccessResponseEntity result = readEntity(response, AccessResponseEntity.class);
            return result.granted;
        });
    }

    @Override
    public void addPolicyRule(AthenzDomain athenzDomain, String athenzPolicy, String action, AthenzResourceName resourceName, AthenzRole athenzRole) {
        URI uri = zmsUrl.resolve(String.format("domain/%s/policy/%s/assertion",
                athenzDomain.getName(), athenzPolicy));
        HttpUriRequest request = RequestBuilder.put()
                .setUri(uri)
                .setEntity(toJsonStringEntity(new AssertionEntity(athenzRole.toResourceNameString(), resourceName.toResourceNameString(), action)))
                .build();
        execute(request, response -> readEntity(response, Void.class));
    }

    @Override
    public boolean deletePolicyRule(AthenzDomain athenzDomain, String athenzPolicy, String action, AthenzResourceName resourceName, AthenzRole athenzRole) {
        URI uri = zmsUrl.resolve(String.format("domain/%s/policy/%s",
                athenzDomain.getName(), athenzPolicy));
        HttpUriRequest request = RequestBuilder.get()
                .setUri(uri)
                .build();
        PolicyEntity policyEntity =  execute(request, response -> readEntity(response, PolicyEntity.class));

        OptionalInt assertionId = policyEntity.getAssertions().stream()
                .filter(assertionEntity -> assertionEntity.getAction().equals(action) &&
                        assertionEntity.getResource().equals(resourceName.toResourceNameString()) &&
                        assertionEntity.getRole().equals(athenzRole.toResourceNameString()))
                .mapToInt(AssertionEntity::getId).findFirst();

        if (assertionId.isEmpty()) {
            return false;
        }

        uri = zmsUrl.resolve(String.format("domain/%s/policy/%s/assertion/%d",
                athenzDomain.getName(), athenzPolicy, assertionId.getAsInt()));

        request = RequestBuilder.delete()
                .setUri(uri)
                .build();

        execute(request, response -> readEntity(response, Void.class));
        return true;
    }

    @Override
    public List<AthenzUser> listPendingRoleApprovals(AthenzRole athenzRole) {
        URI uri = zmsUrl.resolve(String.format("domain/%s/role/%s?pending=true", athenzRole.domain().getName(), athenzRole.roleName()));
        HttpUriRequest request = RequestBuilder.get()
                .setUri(uri)
                .build();
        RoleEntity roleEntity =  execute(request, response -> readEntity(response, RoleEntity.class));
        return roleEntity.roleMembers().stream()
                .filter(RoleEntity.Member::pendingApproval)
                .map(RoleEntity.Member::memberName)
                .map(AthenzIdentities::from)
                .filter(identity -> AthenzIdentities.USER_PRINCIPAL_DOMAIN.equals(identity.getDomain()))
                .map(AthenzUser.class::cast)
                .collect(Collectors.toList());
    }

    @Override
    public void approvePendingRoleMembership(AthenzRole athenzRole, AthenzUser athenzUser, Instant expiry) {
        URI uri = zmsUrl.resolve(String.format("domain/%s/role/%s/member/%s/decision", athenzRole.domain().getName(), athenzRole.roleName(), athenzUser.getFullName()));
        MembershipEntity membership = new MembershipEntity(athenzUser.getFullName(), true, athenzRole.roleName(), Long.toString(expiry.getEpochSecond()));
        HttpUriRequest request = RequestBuilder.put()
                .setUri(uri)
                .setEntity(toJsonStringEntity(membership))
                .build();
        execute(request, response -> readEntity(response, Void.class));
    }

    @Override
    public List<AthenzIdentity> listMembers(AthenzRole athenzRole) {
        URI uri = zmsUrl.resolve(String.format("domain/%s/role/%s", athenzRole.domain().getName(), athenzRole.roleName()));
        RoleEntity execute = execute(RequestBuilder.get(uri).build(), response -> readEntity(response, RoleEntity.class));
        return execute.roleMembers().stream()
                .filter(member -> ! member.pendingApproval())
                .map(RoleEntity.Member::memberName)
                .map(AthenzIdentities::from)
                .collect(Collectors.toList());
    }

    private static Header createCookieHeaderWithOktaTokens(OktaIdentityToken identityToken, OktaAccessToken accessToken) {
        return new BasicHeader("Cookie", String.format("okta_at=%s; okta_it=%s", accessToken.token(), identityToken.token()));
    }

}
