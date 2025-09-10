package org.bettingengine.bettingengine;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.web.client.RestClient;

import java.nio.file.Files;
import java.nio.file.Path;
import java.text.Normalizer;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class TournamentTeamPlaysParserImplTest {

    private static final String TOKEN =
            "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJTdWJqZWN0IjoiNTZhMWUyNTEtOTUxMy00YTcxLWI4ZmEtOWIzZjI5ZjU4MDk0IiwiU3RlYW1JZCI6IjExMzI3Mzk5NjYiLCJBUElVc2VyIjoidHJ1ZSIsIm5iZiI6MTc1NzQwNzYxNCwiZXhwIjoxNzg4OTQzNjE0LCJpYXQiOjE3NTc0MDc2MTQsImlzcyI6Imh0dHBzOi8vYXBpLnN0cmF0ei5jb20ifQ.QjYFmKFZk6uE1-Xfz5UijBYIu4udMO1Lr7shmojmHIU";

    TournamentTeamPlaysParserImpl parser = new TournamentTeamPlaysParserImpl(RestClient.builder()
            .baseUrl("https://api.stratz.com/graphql")
            .defaultHeader(HttpHeaders.AUTHORIZATION, "Bearer " + TOKEN)
            .defaultHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .defaultHeader(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE)
            .defaultHeader("User-Agent", "STRATZ_API")
            .build());

    private final ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

    private static final int INT_25_ID = 18324;
    List<Long> teamIds = List.of(36L,
            2163L,
            7119388L,
            7554697L,
            7732977L,
            8255888L,
            8261500L,
            8291895L,
            9247354L,
            9303484L,
            9351740L,
            9467224L,
            9572001L,
            9640842L,
            9651185L,
            9691969L /* Nemesis */);

    /** STEP 1 — Fetch & dump raw GraphQL for all teams (one JSON per team). */
    @Test
    void fetchHistoricalData() throws Exception {
        Path outDir = Path.of("out/ti25_raw");
        Files.createDirectories(outDir);

        for (Long teamId : teamIds) {
            try {
                var file = parser.dumpTeamRawJson(teamId.intValue(), /*take*/ 100, /*skip*/ 0, outDir);
                System.out.println("Wrote " + file.toAbsolutePath());
            } catch (Exception e) {
                System.err.println("Failed to dump raw for teamId=" + teamId + " : " + e.getMessage());
            }
        }
    }

    /** STEP 2 — Read raw files and write analyzed files to a separate directory. */
    @Test
    void expandHistoricalJsons() throws Exception {
        Path inDir = Path.of("out/ti25_raw");
        Path outDir = Path.of("out/ti25_analyzed");
        Files.createDirectories(outDir);

        TeamJsonAnalyzer analyzer = new TeamJsonAnalyzer();
        ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

        try (Stream<Path> files = Files.list(inDir)) {
            files.filter(p -> p.getFileName().toString().endsWith(".json")).forEach(p -> {
                try {
                    var analyzed = analyzer.analyzeTeamFile(p, INT_25_ID);
                    String outName = p.getFileName().toString().replace(".json", "-analyzed.json");
                    Path outFile = outDir.resolve(outName);
                    mapper.writeValue(outFile.toFile(), analyzed);
                    System.out.println("Wrote " + outFile.toAbsolutePath());
                } catch (Exception e) {
                    System.err.println("Failed to analyze " + p + " : " + e.getMessage());
                }
            });
        }
    }

    @Test
    void fetchPlayersCareerData() throws Exception {
        Path rawDir = Path.of("out/ti25_raw");          // where your team raw JSONs live
        Path outFile = Path.of("out/pro_career/players_pro_career.json");

        Set<Long> steamIds = collectSteamIdsFromRawTeams(rawDir);
        System.out.println("Collected Steam IDs: " + steamIds.size());

        parser.dumpPlayersCareerJson(steamIds, /*takeHeroes*/ 10, outFile);
        System.out.println("Wrote " + outFile.toAbsolutePath());
    }

    /** Collect all steamAccount.id values from team raw JSONs. */
    @SuppressWarnings("unchecked")
    private Set<Long> collectSteamIdsFromRawTeams(Path dir) throws Exception {
        Set<Long> ids = new LinkedHashSet<>();
        try (Stream<Path> files = Files.list(dir)) {
            for (Path p : files.filter(f -> f.getFileName().toString().endsWith(".json")).toList()) {
                Map<String,Object> doc = mapper.readValue(Files.readAllBytes(p), new TypeReference<>() {});
                Map<String,Object> data = (Map<String,Object>) doc.get("data");
                if (data == null) continue;
                Map<String,Object> team = (Map<String,Object>) data.get("team");
                if (team == null) continue;
                List<Map<String,Object>> matches = (List<Map<String,Object>>) team.get("matches");
                if (matches == null) continue;
                for (var m : matches) {
                    List<Map<String,Object>> players = (List<Map<String,Object>>) m.get("players");
                    if (players == null) continue;
                    for (var pl : players) {
                        Map<String,Object> sa = (Map<String,Object>) pl.get("steamAccount");
                        if (sa == null) continue;
                        Object idObj = sa.get("id");
                        if (idObj instanceof Number n) ids.add(n.longValue());
                        else if (idObj instanceof String s) try { ids.add(Long.parseLong(s)); } catch (Exception ignored) {}
                    }
                }
            }
        }
        return ids;
    }

    /** Load {data:{players:[...]}} and index by steamAccount.id. */
    @SuppressWarnings("unchecked")
    private Map<Long, Map<String,Object>> loadCareerMap(Path playersCareerFile) throws Exception {
        Map<String,Object> root = mapper.readValue(Files.readAllBytes(playersCareerFile), new TypeReference<>() {});
        Map<String,Object> data = (Map<String,Object>) root.get("data");
        List<Map<String,Object>> players = data == null ? List.of() : (List<Map<String,Object>>) data.get("players");
        Map<Long, Map<String,Object>> map = new LinkedHashMap<>();
        for (var p : players) {
            Map<String,Object> sa = (Map<String,Object>) p.get("steamAccount");
            if (sa == null) continue;
            Object idObj = sa.get("id");
            Long sid = (idObj instanceof Number n) ? n.longValue()
                    : (idObj instanceof String s ? Long.valueOf(s) : null);
            if (sid != null) map.put(sid, p);
        }
        return map;
    }

    /** Inject 'proCareer' block into each players[*] node using steamAccount.id match. */
    @SuppressWarnings("unchecked")
    private void injectCareerIntoTeamFile(Map<String,Object> teamDoc, Map<Long, Map<String,Object>> careerBySteam) {
        Map<String,Object> data = (Map<String,Object>) teamDoc.get("data");
        if (data == null) return;
        Map<String,Object> team = (Map<String,Object>) data.get("team");
        if (team == null) return;
        List<Map<String,Object>> matches = (List<Map<String,Object>>) team.get("matches");
        if (matches == null) return;

        for (var m : matches) {
            List<Map<String,Object>> players = (List<Map<String,Object>>) m.get("players");
            if (players == null) continue;

            for (var pl : players) {
                Map<String,Object> sa = (Map<String,Object>) pl.get("steamAccount");
                if (sa == null) continue;
                Object idObj = sa.get("id");
                Long sid = (idObj instanceof Number n) ? n.longValue()
                        : (idObj instanceof String s ? parseLongSafe(s) : null);
                if (sid == null) continue;

                Map<String,Object> career = careerBySteam.get(sid);
                if (career == null) continue;

                // Keep the payload compact: attach only what we need
                Map<String,Object> proCareer = Map.of(
                        "steamId", sid,
                        "proName", Optional.ofNullable((Map<String,Object>) sa.get("proSteamAccount"))
                                .map(x -> (String) x.get("name")).orElse(null),
                        "heroesPerformance", career.get("heroesPerformance")
                );
                pl.put("proCareer", proCareer);
            }
        }
    }

    private static Long parseLongSafe(String s) { try { return Long.valueOf(s); } catch (Exception e) { return null; } }

}