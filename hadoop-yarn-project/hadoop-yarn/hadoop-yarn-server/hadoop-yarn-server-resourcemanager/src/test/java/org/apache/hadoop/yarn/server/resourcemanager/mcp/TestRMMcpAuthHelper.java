/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.mcp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.server.resourcemanager.mcp.apikey.RMMcpApiKeyManager;
import org.apache.hadoop.yarn.server.resourcemanager.mcp.apikey.RMMcpApiKeyManagerTestHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public class TestRMMcpAuthHelper {

  @AfterEach
  public void tearDown() {
    Configuration conf = new Configuration();
    conf.set("hadoop.security.authentication", "simple");
    UserGroupInformation.setConfiguration(conf);
  }

  @Test
  public void testApiKeyHeaderAuthentication() throws Exception {
    enableSecureCluster();
    RMMcpApiKeyManager manager = RMMcpApiKeyManagerTestHelper.newManager();
    RMMcpApiKeyManager.CreateResult created =
        manager.createKey("alice", "admin");
    MockHttpServletRequest request = new MockHttpServletRequest();
    request.addHeader(RMMcpToolExecutor.API_KEY_HEADER, created.getApiKey());

    UserGroupInformation caller = RMMcpToolExecutor.resolveCallerUgi(request, manager);
    assertNotNull(caller);
    assertEquals("alice", caller.getShortUserName());
  }

  @Test
  public void testAuthorizationApiKeyHeader() throws Exception {
    enableSecureCluster();
    RMMcpApiKeyManager manager = RMMcpApiKeyManagerTestHelper.newManager();
    RMMcpApiKeyManager.CreateResult created =
        manager.createKey("alice", "admin");
    MockHttpServletRequest request = new MockHttpServletRequest();
    request.addHeader("Authorization", "ApiKey " + created.getApiKey());

    UserGroupInformation caller = RMMcpToolExecutor.resolveCallerUgi(request, manager);
    assertNotNull(caller);
    assertEquals("alice", caller.getShortUserName());
  }

  @Test
  public void testIgnoresApiKeyWhenUnsecure() throws Exception {
    RMMcpApiKeyManager manager = RMMcpApiKeyManagerTestHelper.newManager();
    RMMcpApiKeyManager.CreateResult created =
        manager.createKey("alice", "admin");
    MockHttpServletRequest request = new MockHttpServletRequest();
    request.addHeader(RMMcpToolExecutor.API_KEY_HEADER, created.getApiKey());

    assertNull(RMMcpToolExecutor.resolveCallerUgi(request, manager));
  }

  private static void enableSecureCluster() {
    Configuration conf = new Configuration();
    conf.set("hadoop.security.authentication", "kerberos");
    UserGroupInformation.setConfiguration(conf);
  }

  private static final class MockHttpServletRequest
      implements javax.servlet.http.HttpServletRequest {
    private final java.util.Map<String, String> headers = new java.util.HashMap<>();

    void addHeader(String name, String value) {
      headers.put(name, value);
    }

    @Override
    public String getHeader(String name) {
      return headers.get(name);
    }

    @Override
    public java.util.Enumeration<String> getHeaderNames() {
      return java.util.Collections.enumeration(headers.keySet());
    }

    @Override public Object getAttribute(String name) { return null; }
    @Override public java.util.Enumeration<String> getAttributeNames() { return null; }
    @Override public String getCharacterEncoding() { return null; }
    @Override public void setCharacterEncoding(String env) { }
    @Override public int getContentLength() { return 0; }
    @Override public long getContentLengthLong() { return 0; }
    @Override public String getContentType() { return null; }
    @Override public javax.servlet.ServletInputStream getInputStream() { return null; }
    @Override public String getParameter(String name) { return null; }
    @Override public java.util.Enumeration<String> getParameterNames() { return null; }
    @Override public String[] getParameterValues(String name) { return null; }
    @Override public java.util.Map<String, String[]> getParameterMap() { return null; }
    @Override public String getProtocol() { return null; }
    @Override public String getScheme() { return null; }
    @Override public String getServerName() { return null; }
    @Override public int getServerPort() { return 0; }
    @Override public java.io.BufferedReader getReader() { return null; }
    @Override public String getRemoteAddr() { return null; }
    @Override public String getRemoteHost() { return null; }
    @Override public void setAttribute(String name, Object o) { }
    @Override public void removeAttribute(String name) { }
    @Override public java.util.Locale getLocale() { return null; }
    @Override public java.util.Enumeration<java.util.Locale> getLocales() { return null; }
    @Override public boolean isSecure() { return false; }
    @Override public javax.servlet.RequestDispatcher getRequestDispatcher(String path) {
      return null;
    }
    @Override public String getRealPath(String path) { return null; }
    @Override public int getRemotePort() { return 0; }
    @Override public String getLocalName() { return null; }
    @Override public String getLocalAddr() { return null; }
    @Override public int getLocalPort() { return 0; }
    @Override public javax.servlet.ServletContext getServletContext() { return null; }
    @Override public javax.servlet.AsyncContext startAsync() { return null; }
    @Override public javax.servlet.AsyncContext startAsync(
        javax.servlet.ServletRequest servletRequest,
        javax.servlet.ServletResponse servletResponse) { return null; }
    @Override public boolean isAsyncStarted() { return false; }
    @Override public boolean isAsyncSupported() { return false; }
    @Override public javax.servlet.AsyncContext getAsyncContext() { return null; }
    @Override public javax.servlet.DispatcherType getDispatcherType() { return null; }
    @Override public String getAuthType() { return null; }
    @Override public javax.servlet.http.Cookie[] getCookies() { return null; }
    @Override public long getDateHeader(String name) { return 0; }
    @Override public java.util.Enumeration<String> getHeaders(String name) { return null; }
    @Override public int getIntHeader(String name) { return 0; }
    @Override public String getMethod() { return null; }
    @Override public String getPathInfo() { return null; }
    @Override public String getPathTranslated() { return null; }
    @Override public String getContextPath() { return null; }
    @Override public String getQueryString() { return null; }
    @Override public String getRemoteUser() { return null; }
    @Override public boolean isUserInRole(String role) { return false; }
    @Override public java.security.Principal getUserPrincipal() { return null; }
    @Override public String getRequestedSessionId() { return null; }
    @Override public String getRequestURI() { return null; }
    @Override public StringBuffer getRequestURL() { return null; }
    @Override public String getServletPath() { return null; }
    @Override public javax.servlet.http.HttpSession getSession(boolean create) { return null; }
    @Override public javax.servlet.http.HttpSession getSession() { return null; }
    @Override public String changeSessionId() { return null; }
    @Override public boolean isRequestedSessionIdValid() { return false; }
    @Override public boolean isRequestedSessionIdFromCookie() { return false; }
    @Override public boolean isRequestedSessionIdFromURL() { return false; }
    @Override public boolean isRequestedSessionIdFromUrl() { return false; }
    @Override public boolean authenticate(javax.servlet.http.HttpServletResponse response) {
      return false;
    }
    @Override public void login(String username, String password) { }
    @Override public void logout() { }
    @Override public java.util.Collection<javax.servlet.http.Part> getParts() { return null; }
    @Override public javax.servlet.http.Part getPart(String name) { return null; }
    @Override public <T extends javax.servlet.http.HttpUpgradeHandler> T upgrade(
        Class<T> handlerClass) { return null; }
  }
}
