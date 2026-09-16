package com.engine.taskflow.config;

import com.engine.taskflow.dto.ErrorResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.LocalDateTime;
import java.util.List;

@Component
@Slf4j
public class ApiKeyAuthFilter extends OncePerRequestFilter {

    public static final String API_KEY_HEADER = "X-API-KEY";

    private final String expectedApiKey;
    private final String expectedAdminApiKey;
    private final ObjectMapper objectMapper;

    public ApiKeyAuthFilter(
            @Value("${app.security.api-key}") String expectedApiKey,
            @Value("${app.security.admin-api-key}") String expectedAdminApiKey,
            ObjectMapper objectMapper) {
        this.expectedApiKey = expectedApiKey;
        this.expectedAdminApiKey = expectedAdminApiKey;
        this.objectMapper = objectMapper;
    }

    @PostConstruct
    public void validateApiKeys() {
        if (expectedApiKey == null || expectedApiKey.isBlank()) {
            throw new IllegalStateException(
                    "Security configuration error: 'app.security.api-key' (or APP_SECURITY_API_KEY environment variable) is required and must not be blank.");
        }
        if (expectedAdminApiKey == null || expectedAdminApiKey.isBlank()) {
            throw new IllegalStateException(
                    "Security configuration error: 'app.security.admin-api-key' (or APP_SECURITY_ADMIN_API_KEY environment variable) is required and must not be blank.");
        }
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        String path = request.getRequestURI();
        // Allow static resources, welcome page, and error endpoints without API key
        return !path.startsWith("/api/v1/");
    }

    @Override
    protected void doFilterInternal(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain filterChain) throws ServletException, IOException {

        String requestApiKey = request.getHeader(API_KEY_HEADER);

        if (requestApiKey == null) {
            sendUnauthorized(request, response, "MISSING");
            return;
        }

        byte[] requestBytes = requestApiKey.getBytes(StandardCharsets.UTF_8);

        boolean isAdmin = expectedAdminApiKey != null && MessageDigest.isEqual(
                requestBytes,
                expectedAdminApiKey.getBytes(StandardCharsets.UTF_8)
        );

        boolean isRegularUser = expectedApiKey != null && MessageDigest.isEqual(
                requestBytes,
                expectedApiKey.getBytes(StandardCharsets.UTF_8)
        );

        if (!isAdmin && !isRegularUser) {
            sendUnauthorized(request, response, "INVALID");
            return;
        }

        List<SimpleGrantedAuthority> authorities;
        String principal;

        if (isAdmin) {
            // Admin key gets both ROLE_ADMIN and ROLE_API_USER
            authorities = List.of(
                    new SimpleGrantedAuthority("ROLE_ADMIN"),
                    new SimpleGrantedAuthority("ROLE_API_USER")
            );
            principal = "apiKeyAdmin";
        } else {
            // Regular key gets ROLE_API_USER
            authorities = List.of(
                    new SimpleGrantedAuthority("ROLE_API_USER")
            );
            principal = "apiKeyUser";
        }

        UsernamePasswordAuthenticationToken authentication = new UsernamePasswordAuthenticationToken(
                principal,
                null,
                authorities
        );
        SecurityContextHolder.getContext().setAuthentication(authentication);

        filterChain.doFilter(request, response);
    }

    private void sendUnauthorized(HttpServletRequest request, HttpServletResponse response, String reason) throws IOException {
        log.warn("Unauthorized API access attempt to [{}] with {} header: {}",
                request.getRequestURI(), API_KEY_HEADER, reason);

        response.setStatus(HttpStatus.UNAUTHORIZED.value());
        response.setContentType(MediaType.APPLICATION_JSON_VALUE);

        ErrorResponse errorResponse = ErrorResponse.builder()
                .timestamp(LocalDateTime.now())
                .status(HttpStatus.UNAUTHORIZED.value())
                .error("Unauthorized")
                .message("Invalid or missing API key in " + API_KEY_HEADER + " header")
                .path(request.getRequestURI())
                .build();

        response.getWriter().write(objectMapper.writeValueAsString(errorResponse));
    }
}
