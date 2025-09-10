package org.bettingengine.bettingengine;

import java.util.Map;

public record GraphQLRequest(String query, Map<String, Object> variables) {}
