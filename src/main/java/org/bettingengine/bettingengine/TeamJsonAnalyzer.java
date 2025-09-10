package org.bettingengine.bettingengine;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Reads raw team JSON files (as dumped by TournamentTeamPlaysParserImpl)
 * and computes: wins/losses, avg/median durations, team kills/deaths, per-player best heroes.
 * Also maps towerDeaths.npcId -> human name using OBJECTIVE_BY_ID.
 */
public class TeamJsonAnalyzer {

    private final ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

    // Same objective map you already curated
    private static final Map<Integer, String> OBJECTIVE_BY_ID = Map.ofEntries(
            Map.entry(133, "Roshan"), Map.entry(134, "Roshan (Halloween)"), Map.entry(135, "Roshan Minion (seasonal)"),
            Map.entry(16, "Radiant Tower T1 Top"), Map.entry(17, "Radiant Tower T1 Mid"), Map.entry(18, "Radiant Tower T1 Bot"),
            Map.entry(19, "Radiant Tower T2 Top"), Map.entry(20, "Radiant Tower T2 Mid"), Map.entry(21, "Radiant Tower T2 Bot"),
            Map.entry(22, "Radiant Tower T3 Top"), Map.entry(23, "Radiant Tower T3 Mid"), Map.entry(24, "Radiant Tower T3 Bot"),
            Map.entry(25, "Radiant Tower T4"),
            Map.entry(26, "Dire Tower T1 Top"), Map.entry(27, "Dire Tower T1 Mid"), Map.entry(28, "Dire Tower T1 Bot"),
            Map.entry(29, "Dire Tower T2 Top"), Map.entry(30, "Dire Tower T2 Mid"), Map.entry(31, "Dire Tower T2 Bot"),
            Map.entry(32, "Dire Tower T3 Top"), Map.entry(33, "Dire Tower T3 Mid"), Map.entry(34, "Dire Tower T3 Bot"),
            Map.entry(35, "Dire Tower T4"),
            Map.entry(38, "Radiant Melee Rax Top"), Map.entry(39, "Radiant Melee Rax Mid"), Map.entry(40, "Radiant Melee Rax Bot"),
            Map.entry(41, "Radiant Range Rax Top"), Map.entry(42, "Radiant Range Rax Mid"), Map.entry(43, "Radiant Range Rax Bot"),
            Map.entry(44, "Dire Melee Rax Top"), Map.entry(45, "Dire Melee Rax Mid"), Map.entry(46, "Dire Melee Rax Bot"),
            Map.entry(47, "Dire Range Rax Top"), Map.entry(48, "Dire Range Rax Mid"), Map.entry(49, "Dire Range Rax Bot"),
            Map.entry(50, "Radiant Ancient"), Map.entry(51, "Dire Ancient"),
            Map.entry(822, "Watch Tower"), Map.entry(864, "Twin Gate"), Map.entry(888, "Lotus Pool"),
            Map.entry(861, "Tormentor"), Map.entry(890, "Tormentor Minion (ignore for kill attribution)")
    );

    // ---------- Public API ----------

    /** Analyze a single raw team file and return an augmented structure (filtered by tournament). */
    @SuppressWarnings("unchecked")
    public Map<String, Object> analyzeTeamFile(Path file, int tournamentId) throws IOException {
        Map<String, Object> resp = mapper.readValue(Files.readAllBytes(file), new TypeReference<>() {});
        Map<String, Object> data = (Map<String, Object>) resp.get("data");
        if (data == null) return Map.of("data", Map.of());

        Map<String, Object> team = (Map<String, Object>) data.get("team");
        if (team == null) return resp;

        Integer teamId = toInt(team.get("id"));
        List<Map<String, Object>> matches = (List<Map<String, Object>>) team.get("matches");
        if (matches == null) { team.put("matches", List.of()); return resp; }

        // 1) Filter matches by tournament (league.id == tournamentId)
        List<Map<String, Object>> filtered = matches.stream()
                .filter(m -> {
                    Map<String, Object> league = (Map<String, Object>) m.get("league");
                    return league != null && Objects.equals(toInt(league.get("id")), tournamentId);
                })
                .collect(Collectors.toCollection(ArrayList::new));

        // 2) Map towerDeaths.npcId -> npcName (add a field)
        for (var m : filtered) {
            List<Map<String, Object>> towerDeaths = (List<Map<String, Object>>) m.get("towerDeaths");
            if (towerDeaths == null) continue;
            for (var ev : towerDeaths) {
                Integer npcId = toInt(ev.get("npcId"));
                if (npcId == null) continue;
                ev.put("npcName", OBJECTIVE_BY_ID.getOrDefault(npcId, "npc#" + npcId));
            }
        }

        // 3) Team-level aggregates
        int wins = 0, losses = 0;
        List<Integer> durations = new ArrayList<>();
        List<Integer> teamKillsPerMatch = new ArrayList<>();
        List<Integer> teamDeathsPerMatch = new ArrayList<>();

        // 4) Per-player aggregates keyed by pro nickname (fallback "Unknown")
        Map<String, PlayerAgg> perPlayer = new LinkedHashMap<>();

        for (var m : filtered) {
            Map<String, Object> rad = (Map<String, Object>) m.get("radiantTeam");
            Map<String, Object> dire = (Map<String, Object>) m.get("direTeam");
            Integer radId = rad != null ? toInt(rad.get("id")) : null;
            Integer dirId = dire != null ? toInt(dire.get("id")) : null;
            if (radId == null || dirId == null || teamId == null) continue;

            boolean teamIsRad = teamId.equals(radId);
            boolean teamIsDire = teamId.equals(dirId);

            Boolean didRadiantWin = toBool(m.get("didRadiantWin"));
            if (didRadiantWin != null) {
                boolean win = (teamIsRad && didRadiantWin) || (teamIsDire && !didRadiantWin);
                if (win) wins++; else losses++;
            }

            Integer dur = toInt(m.get("durationSeconds"));
            if (dur != null) durations.add(dur);

            List<Map<String, Object>> players = (List<Map<String, Object>>) m.get("players");
            int killsSum = 0, deathsSum = 0;
            if (players != null) {
                for (var p : players) {
                    Boolean isRad = toBool(p.get("isRadiant"));
                    if (isRad == null) continue;
                    boolean isOnTeam = (isRad && teamIsRad) || (!isRad && teamIsDire);
                    if (!isOnTeam) continue;

                    int k = nvlInt(p.get("kills"));
                    int d = nvlInt(p.get("deaths"));
                    killsSum += k; deathsSum += d;

                    Map<String, Object> hero = (Map<String, Object>) p.get("hero");
                    Integer heroId = hero != null ? toInt(hero.get("id")) : null;
                    String heroName = hero != null ? Objects.toString(hero.get("displayName"), "") : "";

                    String playerName = "Unknown";
                    Map<String, Object> sa = (Map<String, Object>) p.get("steamAccount");
                    if (sa != null) {
                        Map<String, Object> pro = (Map<String, Object>) sa.get("proSteamAccount");
                        if (pro != null) playerName = Objects.toString(pro.get("name"), "Unknown");
                    }

                    boolean victory = Boolean.TRUE.equals(toBool(p.get("isVictory")));

                    PlayerAgg agg = perPlayer.computeIfAbsent(playerName, PlayerAgg::new);
                    agg.kills.add(k);
                    agg.deaths.add(d);
                    agg.matches++;
                    if (victory) agg.wins++;

                    if (heroId != null) {
                        final String hName = heroName; // effectively final for lambda
                        HeroAgg ha = agg.heroes.computeIfAbsent(heroId, id2 -> new HeroAgg(id2, hName));
                        ha.matches++;
                        if (victory) ha.wins++;
                        ha.kills.add(k);
                        ha.deaths.add(d);
                    }
                }
            }
            teamKillsPerMatch.add(killsSum);
            teamDeathsPerMatch.add(deathsSum);
        }

        Map<String, Object> teamAgg = Map.of(
                "matchesCount", filtered.size(),
                "wins", wins,
                "losses", losses,
                "durationSeconds", Map.of("average", average(durations), "median", median(durations)),
                "kills", Map.of("average", average(teamKillsPerMatch), "median", median(teamKillsPerMatch)),
                "deaths", Map.of("average", average(teamDeathsPerMatch), "median", median(teamDeathsPerMatch))
        );

        // 5) Build per-player output with best heroes (win rate -> picks -> name)
        List<Map<String, Object>> playerAggregates = perPlayer.values().stream()
                .map(pa -> {
                    List<Map<String, Object>> bestHeroes = pa.heroes.values().stream()
                            .sorted((a, b) -> {
                                double wrA = a.matches > 0 ? (double) a.wins / a.matches : 0.0;
                                double wrB = b.matches > 0 ? (double) b.wins / b.matches : 0.0;
                                int byWr = Double.compare(wrB, wrA); if (byWr != 0) return byWr;
                                int byPicks = Integer.compare(b.matches, a.matches); if (byPicks != 0) return byPicks;
                                String an = a.heroName != null ? a.heroName : "";
                                String bn = b.heroName != null ? b.heroName : "";
                                return an.compareTo(bn);
                            })
                            .limit(3)
                            .map(h -> Map.of(
                                    "heroId", h.heroId,
                                    "heroName", h.heroName,
                                    "matches", h.matches,
                                    "wins", h.wins,
                                    "winRate", h.matches > 0 ? (double) h.wins / h.matches : 0.0,
                                    "kills", Map.of("average", average(h.kills), "median", median(h.kills)),
                                    "deaths", Map.of("average", average(h.deaths), "median", median(h.deaths))
                            ))
                            .toList();

                    return Map.of(
                            "playerName", pa.playerName,
                            "matchesCount", pa.matches,
                            "wins", pa.wins,
                            "kills", Map.of("average", average(pa.kills), "median", median(pa.kills)),
                            "deaths", Map.of("average", average(pa.deaths), "median", median(pa.deaths)),
                            "bestHeroes", bestHeroes
                    );
                })
                .toList();

        // Replace matches with filtered and attach aggregates
        team.put("matches", filtered);
        team.put("aggregates", teamAgg);
        team.put("playerAggregates", playerAggregates);

        return resp;
    }

    /** Analyze every *.json file in a directory and write <name>-analyzed.json next to each. */
    public void analyzeDirectory(Path dir, int tournamentId) throws IOException {
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(dir, "*.json")) {
            for (Path p : ds) {
                Map<String, Object> analyzed = analyzeTeamFile(p, tournamentId);
                Path out = p.resolveSibling(p.getFileName().toString().replace(".json", "-analyzed.json"));
                mapper.writeValue(out.toFile(), analyzed);
                System.out.println("Wrote " + out.toAbsolutePath());
            }
        }
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
    private static int nvlInt(Object o) { Integer v = toInt(o); return v != null ? v : 0; }
    private static double average(List<Integer> values) {
        if (values == null || values.isEmpty()) return 0.0;
        long sum = 0L; for (int v : values) sum += v; return sum / (double) values.size();
    }
    private static double median(List<Integer> values) {
        if (values == null || values.isEmpty()) return 0.0;
        List<Integer> s = new ArrayList<>(values); s.sort(Integer::compareTo);
        int n = s.size(); return (n % 2 == 1) ? s.get(n/2) : (s.get(n/2-1) + s.get(n/2)) / 2.0;
    }

    // aggregation containers
    private static final class PlayerAgg {
        final String playerName;
        int matches = 0, wins = 0;
        final List<Integer> kills = new ArrayList<>();
        final List<Integer> deaths = new ArrayList<>();
        final Map<Integer, HeroAgg> heroes = new LinkedHashMap<>();
        PlayerAgg(String name) { this.playerName = name; }
    }
    private static final class HeroAgg {
        final int heroId; final String heroName;
        int matches = 0, wins = 0;
        final List<Integer> kills = new ArrayList<>();
        final List<Integer> deaths = new ArrayList<>();
        HeroAgg(int id, String name) { this.heroId = id; this.heroName = name; }
    }
}
