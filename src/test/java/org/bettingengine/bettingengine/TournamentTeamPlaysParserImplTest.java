package org.bettingengine.bettingengine;

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
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.*;

class TournamentTeamPlaysParserImplTest {

    private static final String TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJTdWJqZWN0IjoiNTZhMWUyNTEtOTUxMy00YTcxLWI4ZmEtOWIzZjI5ZjU4MDk0IiwiU3RlYW1JZCI6IjExMzI3Mzk5NjYiLCJBUElVc2VyIjoidHJ1ZSIsIm5iZiI6MTc1NzQwNzYxNCwiZXhwIjoxNzg4OTQzNjE0LCJpYXQiOjE3NTc0MDc2MTQsImlzcyI6Imh0dHBzOi8vYXBpLnN0cmF0ei5jb20ifQ.QjYFmKFZk6uE1-Xfz5UijBYIu4udMO1Lr7shmojmHIU";

    TournamentTeamPlaysParserImpl parser = new TournamentTeamPlaysParserImpl(RestClient.builder()
            .baseUrl("https://api.stratz.com/graphql")
            .defaultHeader(HttpHeaders.AUTHORIZATION, "Bearer " + TOKEN)
            .defaultHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .defaultHeader(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE)
            .defaultHeader("User-Agent", "STRATZ_API")
            .build());

    private static final int INT_25_ID = 18324;
    List<Long> teamIds = List.of(
            36L, 2163L, 7119388L, 7554697L, 7732977L, 8255888L, 8261500L, 8291895L,
            9247354L, 9303484L, 9351740L, 9467224L, 9572001L, 9640842L, 9651185L, 9691969L
    );


    @Test
    void dumpTeamsAsIndividualFiles() throws Exception {
        Path outDir = Path.of("out/ti25_teams");
        Files.createDirectories(outDir);

        ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

        for (Long teamId : teamIds) {
            try {
                Map<String, Object> resp = parser.parse(INT_25_ID, teamId.intValue());
                if (resp == null) continue;

                Map<String, Object> data = cast(resp.get("data"));
                if (data == null) continue;

                Map<String, Object> team = cast(data.get("team"));
                if (team == null) continue;

                // ensure matches array exists (even if empty)
                List<Map<String, Object>> matches = cast(team.get("matches"));
                if (matches == null) team.put("matches", List.of());

                // filename: <id>_<team name>.json (sanitize name)
                long id = ((Number) team.getOrDefault("id", teamId)).longValue();
                String name = Objects.toString(team.get("name"), "");
                String tag  = Objects.toString(team.get("tag"), "");
                String display = !name.isBlank() ? name : (!tag.isBlank() ? tag : "team");

                String fileName = id + "_" + sanitize(display) + ".json";
                Path outFile = outDir.resolve(fileName);

                // (optional) add metadata
                team.putIfAbsent("_tournamentId", INT_25_ID);
                team.put("_generatedAt", Instant.now().toString());

                mapper.writeValue(outFile.toFile(), team);
                System.out.println("Wrote " + outFile.toAbsolutePath());
            } catch (Exception ex) {
                System.err.println("Failed for teamId=" + teamId + ": " + ex.getMessage());
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> T cast(Object o) { return (T) o; }

    private static String sanitize(String s) {
        String n = Normalizer.normalize(s, Normalizer.Form.NFD)
                .replaceAll("\\p{M}+", "");                 // strip accents
        n = n.replaceAll("[^a-zA-Z0-9._-]+", "_");          // non-safe -> _
        n = n.replaceAll("_+", "_").replaceAll("^_|_$", ""); // collapse/trim _
        return n.isBlank() ? "team" : n;
    }

}