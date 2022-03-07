/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.nacos.core.auth;

import com.alibaba.nacos.auth.HttpProtocolAuthService;
import com.alibaba.nacos.auth.annotation.Secured;
import com.alibaba.nacos.auth.config.AuthConfigs;
import com.alibaba.nacos.common.utils.ExceptionUtil;
import com.alibaba.nacos.common.utils.StringUtils;
import com.alibaba.nacos.core.code.ControllerMethodsCache;
import com.alibaba.nacos.core.utils.Loggers;
import com.alibaba.nacos.core.utils.WebUtils;
import com.alibaba.nacos.plugin.auth.api.IdentityContext;
import com.alibaba.nacos.plugin.auth.api.Permission;
import com.alibaba.nacos.plugin.auth.api.Resource;
import com.alibaba.nacos.plugin.auth.exception.AccessException;
import com.alibaba.nacos.sys.env.Constants;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.lang.reflect.Method;

/**
 * Unified filter to handle authentication and authorization.
 *
 * @author nkorange
 * @since 1.2.0
 */
public class AuthFilter implements Filter {
    
    private final AuthConfigs authConfigs;
    
    private final ControllerMethodsCache methodsCache;
    
    private final HttpProtocolAuthService protocolAuthService;
    
    public AuthFilter(AuthConfigs authConfigs, ControllerMethodsCache methodsCache) {
        this.authConfigs = authConfigs;
        this.methodsCache = methodsCache;
        this.protocolAuthService = new HttpProtocolAuthService(authConfigs);
        this.protocolAuthService.initialize();
    }
    
    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        // 判断鉴权是否打开
        if (!authConfigs.isAuthEnabled()) {
            chain.doFilter(request, response);
            // 鉴权未打开,跳过认证
            return;
        }
        
        // 获取 request 请求对象
        HttpServletRequest req = (HttpServletRequest) request;
        // 获取 response 响应对象
        HttpServletResponse resp = (HttpServletResponse) response;
        
        // 判断是否开启鉴权白名单
        if (authConfigs.isEnableUserAgentAuthWhite()) {
            // 如果开启鉴权白名单 只要是 UA 头是 Nacos-Server 就跳过鉴权
            String userAgent = WebUtils.getUserAgent(req);
            if (StringUtils.startsWith(userAgent, Constants.NACOS_SERVER_HEADER)) {
                chain.doFilter(request, response);
                return;
            }
            // 判断 server key 和 server value 在配置文件中是否为空
        } else if (StringUtils.isNotBlank(authConfigs.getServerIdentityKey()) && StringUtils
                .isNotBlank(authConfigs.getServerIdentityValue())) {
            // 从请求头中获取 identity value
            String serverIdentity = req.getHeader(authConfigs.getServerIdentityKey());
            // 判断是否为空
            if (StringUtils.isNotBlank(serverIdentity)) {
                // 校验请求头中获取的 value 是否与再配置文件中配置的 value 是否一致
                if (authConfigs.getServerIdentityValue().equals(serverIdentity)) {
                    chain.doFilter(request, response);
                    return;
                }
                Loggers.AUTH.warn("Invalid server identity value for {} from {}", authConfigs.getServerIdentityKey(),
                        req.getRemoteHost());
            }
        } else {
            resp.sendError(HttpServletResponse.SC_FORBIDDEN,
                    "Invalid server identity key or value, Please make sure set `nacos.core.auth.server.identity.key`"
                            + " and `nacos.core.auth.server.identity.value`, or open `nacos.core.auth.enable.userAgentAuthWhite`");
            return;
        }
        
        try {
            
            // 从缓存中获取 method 对象
            Method method = methodsCache.getMethod(req);
            
            // 如果为空 则跳过验证
            if (method == null) {
                chain.doFilter(request, response);
                return;
            }
            
            // 方法上获取 secured 注解 并且 判断是否开启鉴权
            if (method.isAnnotationPresent(Secured.class) && authConfigs.isAuthEnabled()) {
                
                if (Loggers.AUTH.isDebugEnabled()) {
                    Loggers.AUTH.debug("auth start, request: {} {}", req.getMethod(), req.getRequestURI());
                }
                
                Secured secured = method.getAnnotation(Secured.class);
                // 如果没有开启鉴权则跳过鉴权
                if (!protocolAuthService.enableAuth(secured)) {
                    chain.doFilter(request, response);
                    return;
                }
                
                // 解析资源对象
                Resource resource = protocolAuthService.parseResource(req, secured);
                // 创建认证上下文
                IdentityContext identityContext = protocolAuthService.parseIdentity(req);
                // 判断相关用户是否认证通过
                boolean result = protocolAuthService.validateIdentity(identityContext, resource);
                if (!result) {
                    // 鉴权未通过 抛出异常
                    // TODO Get reason of failure
                    throw new AccessException("Validate Identity failed.");
                }
                // 从上下文中获取 identityId 放到 request session 中去
                injectIdentityId(req, identityContext);
                // 获取 action 判断相关动作是否有权限
                String action = secured.action().toString();
                // 判断相关资源的操作是否有权限
                result = protocolAuthService.validateAuthority(identityContext, new Permission(resource, action));
                if (!result) {
                    // TODO Get reason of failure
                    throw new AccessException("Validate Authority failed.");
                }
            }
            chain.doFilter(request, response);
        } catch (AccessException e) {
            if (Loggers.AUTH.isDebugEnabled()) {
                Loggers.AUTH.debug("access denied, request: {} {}, reason: {}", req.getMethod(), req.getRequestURI(),
                        e.getErrMsg());
            }
            resp.sendError(HttpServletResponse.SC_FORBIDDEN, e.getErrMsg());
        } catch (IllegalArgumentException e) {
            resp.sendError(HttpServletResponse.SC_BAD_REQUEST, ExceptionUtil.getAllExceptionMsg(e));
        } catch (Exception e) {
            Loggers.AUTH.warn("[AUTH-FILTER] Server failed: ", e);
            resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Server failed, " + e.getMessage());
        }
    }
    
    /**
     * Set identity id to request session, make sure some actual logic can get identity information.
     *
     * <p>May be replaced with whole identityContext.
     *
     * @param request         http request
     * @param identityContext identity context
     */
    private void injectIdentityId(HttpServletRequest request, IdentityContext identityContext) {
        String identityId = identityContext
                .getParameter(com.alibaba.nacos.plugin.auth.constant.Constants.Identity.IDENTITY_ID, StringUtils.EMPTY);
        request.getSession()
                .setAttribute(com.alibaba.nacos.plugin.auth.constant.Constants.Identity.IDENTITY_ID, identityId);
    }
}
