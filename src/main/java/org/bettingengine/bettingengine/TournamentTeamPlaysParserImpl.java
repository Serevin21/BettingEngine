package org.bettingengine.bettingengine;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.nio.file.Files;
import java.nio.file.Path;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Component
@RequiredArgsConstructor
public class TournamentTeamPlaysParserImpl implements TournamentTeamPlaysParser {

    private static final long STEAM64_OFFSET = 76561197960265728L;
    private final RestClient client;
    private final ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

    // Keep this focused on data you'll want offline (no server-side filtering/aggregation)
    private static final String TEAM_WITH_MATCHES_RAW = """
            query GetTeamWithMatches($teamId: Int!, $take: Int!, $skip: Int!) {
              team(teamId: $teamId) {
                id
                name
                tag
                matches(request: { take: $take, skip: $skip }) {
                  id
                  startDateTime
                  durationSeconds
                  didRadiantWin
                  radiantTeam { id name tag }
                  direTeam    { id name tag }
                  league { id displayName id }
            
                  towerStatusRadiant
                  towerStatusDire
                  barracksStatusRadiant
                  barracksStatusDire
            
                  towerDeaths { time npcId isRadiant }
            
                  players {
                    isRadiant
                    isVictory
                    kills
                    deaths
                    assists
                    goldPerMinute
                    experiencePerMinute
                    networth
            
                    playbackData {
                        killEvents {
                            time
                            target
                        }
                        deathEvents {
                            time
                            attacker
                            goldFed
                            xpFed
                            goldLost
                            isFeed
                            positionX
                            positionY
                        }
                    }
            
                    stats { itemPurchases { time itemId } }
                    hero { id displayName }
                    steamAccount {
                      id
                      name
                      proSteamAccount { id name teamId }
                    }
                  }
                }
              }
              constants {
                  items {
                      id
                      displayName
                      name
                  }
              }
            }
            """;

    private static final String PLAYERS_PRO_ALL_TIME_QUERY = """
            query PlayersProAllTime($ids: [Long!]!, $takeHeroes: Int = 10) {
              players(steamAccountIds: $ids) {
                steamAccount { id proSteamAccount { id name } }
                heroesPerformance(
                  request: { isLeague: true, matchGroupOrderBy: MATCH_COUNT, orderBy: DESC, take: 5000 },
                  take: $takeHeroes
                ) {
                  hero { id displayName }
                  matchCount
                  winCount
                  avgKills
                  avgDeaths
                  avgAssists
                  goldPerMinute
                  experiencePerMinute
                  lastPlayedDateTime
                }
              }
            }
            """;

    /**
     * Fetch players' all-time league performance for provided Steam IDs and write a single JSON.
     * Batches the IDs to avoid huge payloads; merges 'players' arrays into one file.
     */
    @SuppressWarnings("unchecked")
    public Path dumpPlayersCareerJson(Collection<Long> steamIdsRaw, int takeHeroes, Path outFile) throws Exception {
        final int MAX_IDS_PER_REQUEST = 5; // <-- STRATZ limit

        List<Long> ids = normalizeSteamIds(steamIdsRaw);
        if (ids.isEmpty()) {
            throw new IllegalArgumentException("steamIds is empty");
        }

        List<Map<String, Object>> allPlayers = new ArrayList<>();

        for (int i = 0; i < ids.size(); i += MAX_IDS_PER_REQUEST) {
            List<Long> slice = ids.subList(i, Math.min(i + MAX_IDS_PER_REQUEST, ids.size()));

            Map<String, Object> body = Map.of("query", PLAYERS_PRO_ALL_TIME_QUERY, "variables", Map.of("ids", slice, "takeHeroes", takeHeroes));

            Map<String, Object> resp = client.post().body(body).retrieve().body(new org.springframework.core.ParameterizedTypeReference<>() {});

            Object errors = resp.get("errors");
            if (errors != null) {
                System.err.println("GraphQL errors for slice " + slice + " -> " + errors);
            }

            Map<String, Object> data = (Map<String, Object>) resp.get("data");
            if (data == null) {
                continue;
            }

            List<Map<String, Object>> players = (List<Map<String, Object>>) data.get("players");
            if (players != null) {
                allPlayers.addAll(players);
            }

            // Optional: tiny delay to play nice with CF/rate limits
            // Thread.sleep(75);
        }

        // De-dup by steamAccount.id
        Map<Long, Map<String, Object>> byId = new LinkedHashMap<>();
        for (Map<String, Object> p : allPlayers) {
            Map<String, Object> sa = (Map<String, Object>) p.get("steamAccount");
            if (sa == null) {
                continue;
            }
            Object idObj = sa.get("id");
            Long sid = (idObj instanceof Number n) ? n.longValue() : (idObj instanceof String s ? parseLongSafe(s) : null);
            if (sid != null) {
                byId.put(sid, p);
            }
        }

        Map<String, Object> merged = Map.of("data", Map.of("players", new ArrayList<>(byId.values())));

        var mapper = new com.fasterxml.jackson.databind.ObjectMapper().enable(com.fasterxml.jackson.databind.SerializationFeature.INDENT_OUTPUT);
        java.nio.file.Files.createDirectories(outFile.getParent());
        mapper.writeValue(outFile.toFile(), merged);
        return outFile;
    }

    private static Long parseLongSafe(String s) {
        try {
            return Long.valueOf(s);
        } catch (Exception e) {
            return null;
        }
    }

    private static List<Long> normalizeSteamIds(Collection<Long> raw) {
        if (raw == null) {
            return List.of();
        }
        return raw.stream().filter(Objects::nonNull).map(TournamentTeamPlaysParserImpl::toSteam32).distinct().toList();
    }

    private static long toSteam32(long id) {
        return id >= STEAM64_OFFSET ? id - STEAM64_OFFSET : id;
    }

    /**
     * Fetch and dump the raw GraphQL response to: outDir/<teamId>_<teamName>.json
     * No filtering, no mutation. Returns the written file path.
     */
    @SneakyThrows
    @SuppressWarnings("unchecked")
    public Path dumpTeamRawJson(int teamId, int take, int skip, Path outDir) {
        var gql = new GraphQLRequest(TEAM_WITH_MATCHES_RAW, Map.of("teamId", teamId, "take", 15, // or use 'take'
                "skip", 0    // or use 'skip'
        ));

        Map<String, Object> resp = client.post().body(gql).retrieve().body(new ParameterizedTypeReference<>() {});

        var data = (Map<String, Object>) resp.get("data");
        var team = data != null ? (Map<String, Object>) data.get("team") : null;

        long id = (team != null && team.get("id") instanceof Number n) ? n.longValue() : teamId;
        String name = team != null ? Objects.toString(team.get("name"), "") : "";
        String tag = team != null ? Objects.toString(team.get("tag"), "") : "";
        String display = !name.isBlank() ? name : (!tag.isBlank() ? tag : "team");

        String fileName = id + "_" + sanitize(display) + ".json";
        Files.createDirectories(outDir);
        Path outFile = outDir.resolve(fileName);

        mapper.writeValue(outFile.toFile(), resp);
        return outFile;
    }

    private static String sanitize(String s) {
        String n = Normalizer.normalize(s, Normalizer.Form.NFD).replaceAll("\\p{M}+", "");
        n = n.replaceAll("[^a-zA-Z0-9._-]+", "_").replaceAll("_+", "_").replaceAll("^_|_$", "");
        return n.isBlank() ? "team" : n;
    }
}
