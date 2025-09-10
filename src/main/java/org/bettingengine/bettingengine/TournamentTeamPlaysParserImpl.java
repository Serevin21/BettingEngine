package org.bettingengine.bettingengine;

import lombok.RequiredArgsConstructor;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import java.util.*;
import java.util.stream.Collectors;

@Component
@RequiredArgsConstructor
public class TournamentTeamPlaysParserImpl implements TournamentTeamPlaysParser {

    private static final Map<Integer, String> OBJECTIVE_BY_ID = Map.ofEntries(
            // Roshan
            Map.entry(133, "Roshan"),
            Map.entry(134, "Roshan (Halloween)"),
            Map.entry(135, "Roshan Minion (seasonal)"),

            // Radiant towers (goodguys)
            Map.entry(16, "Radiant Tower T1 Top"),
            Map.entry(17, "Radiant Tower T1 Mid"),
            Map.entry(18, "Radiant Tower T1 Bot"),
            Map.entry(19, "Radiant Tower T2 Top"),
            Map.entry(20, "Radiant Tower T2 Mid"),
            Map.entry(21, "Radiant Tower T2 Bot"),
            Map.entry(22, "Radiant Tower T3 Top"),
            Map.entry(23, "Radiant Tower T3 Mid"),
            Map.entry(24, "Radiant Tower T3 Bot"),
            Map.entry(25, "Radiant Tower T4"),

            // Dire towers (badguys)
            Map.entry(26, "Dire Tower T1 Top"),
            Map.entry(27, "Dire Tower T1 Mid"),
            Map.entry(28, "Dire Tower T1 Bot"),
            Map.entry(29, "Dire Tower T2 Top"),
            Map.entry(30, "Dire Tower T2 Mid"),
            Map.entry(31, "Dire Tower T2 Bot"),
            Map.entry(32, "Dire Tower T3 Top"),
            Map.entry(33, "Dire Tower T3 Mid"),
            Map.entry(34, "Dire Tower T3 Bot"),
            Map.entry(35, "Dire Tower T4"),

            // Radiant barracks
            Map.entry(38, "Radiant Melee Rax Top"),
            Map.entry(39, "Radiant Melee Rax Mid"),
            Map.entry(40, "Radiant Melee Rax Bot"),
            Map.entry(41, "Radiant Range Rax Top"),
            Map.entry(42, "Radiant Range Rax Mid"),
            Map.entry(43, "Radiant Range Rax Bot"),

            // Dire barracks
            Map.entry(44, "Dire Melee Rax Top"),
            Map.entry(45, "Dire Melee Rax Mid"),
            Map.entry(46, "Dire Melee Rax Bot"),
            Map.entry(47, "Dire Range Rax Top"),
            Map.entry(48, "Dire Range Rax Mid"),
            Map.entry(49, "Dire Range Rax Bot"),

            // Ancients
            Map.entry(50, "Radiant Ancient"),
            Map.entry(51, "Dire Ancient"),

            // Other major objectives
            Map.entry(822, "Watch Tower"),
            Map.entry(864, "Twin Gate"),
            Map.entry(888, "Lotus Pool"),
            Map.entry(861, "Tormentor"),
            Map.entry(890, "Tormentor Minion (ignore for kill attribution)")
    );

    private static final String getTeamWithMatchesQuery = """
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
              league { id displayName }

              towerStatusRadiant
              towerStatusDire
              barracksStatusRadiant
              barracksStatusDire

              towerDeaths {
                time
                npcId
                isRadiant
              }

              players {
                isRadiant
                kills
                deaths
                assists
                goldPerMinute
                experiencePerMinute
                networth
                isVictory
                stats { itemPurchases { time itemId } }
                hero { id displayName }
                steamAccount { proSteamAccount { name } }
              }
            }
          }
        }
        """;

    private final RestClient client;

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, Object> parse(int tournament, int team) {
        GraphQLRequest gql = new GraphQLRequest(getTeamWithMatchesQuery, Map.of(
                "teamId", team,
                "take", 100,
                "skip", 0
        ));

        Map<String, Object> resp = client.post()
                .body(gql)
                .retrieve()
                .body(new ParameterizedTypeReference<>() {});

        Map<String, Object> data = (Map<String, Object>) resp.get("data");
        if (data == null) return Map.of("data", Map.of());

        Map<String, Object> teamNode = (Map<String, Object>) data.get("team");
        if (teamNode == null) return resp;

        List<Map<String, Object>> matches = (List<Map<String, Object>>) teamNode.get("matches");
        if (matches == null) {
            teamNode.put("matches", List.of());
            return resp;
        }

        // 1) Filter matches by tournament id
        List<Map<String, Object>> filtered = matches.stream()
                .filter(m -> {
                    Object leagueObj = m.get("league");
                    if (!(leagueObj instanceof Map<?, ?> league)) return false;
                    Integer id = toInt(((Map<String, Object>) league).get("id"));
                    return id != null && id == tournament;
                })
                .collect(Collectors.toCollection(ArrayList::new)); // mutable

        // 2) Map towerDeaths[].npcId -> npcName (keeping npcId)
        for (Map<String, Object> match : filtered) {
            List<Map<String, Object>> towerDeaths = (List<Map<String, Object>>) match.get("towerDeaths");
            if (towerDeaths != null) {
                for (Map<String, Object> ev : towerDeaths) {
                    Integer npcId = toInt(ev.get("npcId"));
                    if (npcId != null) {
                        ev.put("npcName", OBJECTIVE_BY_ID.getOrDefault(npcId, "npc#" + npcId));
                    }
                }
            }
        }

        // 3) Compute aggregates for this team (wins/losses, durations, kills, deaths)
        Integer teamId = toInt(teamNode.get("id"));
        if (teamId != null) {
            boolean teamIsRadiantInMatch; // per match check
            int wins = 0, losses = 0;

            List<Integer> durations = new ArrayList<>();
            List<Integer> teamKillsPerMatch = new ArrayList<>();
            List<Integer> teamDeathsPerMatch = new ArrayList<>();

            for (Map<String, Object> match : filtered) {
                Map<String, Object> rad = (Map<String, Object>) match.get("radiantTeam");
                Map<String, Object> dire = (Map<String, Object>) match.get("direTeam");
                Integer radId = rad != null ? toInt(rad.get("id")) : null;
                Integer dirId = dire != null ? toInt(dire.get("id")) : null;

                if (radId == null || dirId == null) continue; // skip malformed

                if (teamId.equals(radId)) {
                    teamIsRadiantInMatch = true;
                } else if (teamId.equals(dirId)) {
                    teamIsRadiantInMatch = false;
                } else {
                    // team not in this match; shouldn't happen after filtering but guard anyway
                    continue;
                }

                // win/loss
                Boolean didRadiantWin = toBool(match.get("didRadiantWin"));
                if (didRadiantWin != null) {
                    boolean win = (teamIsRadiantInMatch == didRadiantWin);
                    if (win) wins++; else losses++;
                }

                // duration
                Integer dur = toInt(match.get("durationSeconds"));
                if (dur != null) durations.add(dur);

                // team kills/deaths per match (sum players on that side)
                List<Map<String, Object>> players = (List<Map<String, Object>>) match.get("players");
                int killsSum = 0, deathsSum = 0;
                if (players != null) {
                    for (Map<String, Object> p : players) {
                        Boolean isRad = toBool(p.get("isRadiant"));
                        if (isRad == null || isRad != teamIsRadiantInMatch) continue;
                        killsSum += nvlInt(p.get("kills"));
                        deathsSum += nvlInt(p.get("deaths"));
                    }
                }
                teamKillsPerMatch.add(killsSum);
                teamDeathsPerMatch.add(deathsSum);
            }

            Map<String, Object> aggregates = Map.of(
                    "matchesCount", filtered.size(),
                    "wins", wins,
                    "losses", losses,
                    "durationSeconds", Map.of(
                            "average", average(durations),
                            "median", median(durations)
                    ),
                    "kills", Map.of(
                            "average", average(teamKillsPerMatch),
                            "median", median(teamKillsPerMatch)
                    ),
                    "deaths", Map.of(
                            "average", average(teamDeathsPerMatch),
                            "median", median(teamDeathsPerMatch)
                    )
            );

            teamNode.put("aggregates", aggregates);
        }

        // 4) Replace matches with filtered and return
        teamNode.put("matches", filtered);
        return resp;
    }

    // ---------- helpers ----------
    private static Integer toInt(Object o) {
        if (o instanceof Number n) return n.intValue();
        if (o instanceof String s) try { return Integer.valueOf(s); } catch (NumberFormatException ignored) {}
        return null;
    }
    private static Boolean toBool(Object o) {
        if (o instanceof Boolean b) return b;
        if (o instanceof Number n) return n.intValue() != 0;
        if (o instanceof String s) return Boolean.parseBoolean(s);
        return null;
    }
    private static int nvlInt(Object o) {
        Integer v = toInt(o);
        return v != null ? v : 0;
    }
    private static double average(List<Integer> values) {
        if (values == null || values.isEmpty()) return 0.0;
        long sum = 0L;
        for (int v : values) sum += v;
        return sum / (double) values.size();
    }
    private static double median(List<Integer> values) {
        if (values == null || values.isEmpty()) return 0.0;
        List<Integer> sorted = new ArrayList<>(values);
        sorted.sort(Integer::compareTo);
        int n = sorted.size();
        if (n % 2 == 1) return sorted.get(n / 2);
        // even: average of two middle values
        return (sorted.get(n / 2 - 1) + sorted.get(n / 2)) / 2.0;
    }
}
