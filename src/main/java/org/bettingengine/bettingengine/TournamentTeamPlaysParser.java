package org.bettingengine.bettingengine;

import java.nio.file.Path;

public interface TournamentTeamPlaysParser {

    Path dumpTeamRawJson(int teamId, int take, int skip, Path outDir);

}
