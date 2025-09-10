package org.bettingengine.bettingengine;

import java.util.Map;

public interface TournamentTeamPlaysParser {

    Map<String, Object> parse(int tournament, int team);

}
