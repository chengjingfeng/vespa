// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.searchdefinition.processing;

import com.yahoo.collections.Pair;
import com.yahoo.config.FileReference;
import com.yahoo.config.model.application.provider.BaseDeployLogger;
import com.yahoo.config.model.deploy.TestProperties;
import com.yahoo.search.query.profile.QueryProfileRegistry;
import com.yahoo.searchdefinition.RankProfile;
import com.yahoo.searchdefinition.RankProfileRegistry;
import com.yahoo.searchdefinition.SchemaTestCase;
import com.yahoo.searchdefinition.Search;
import com.yahoo.searchdefinition.SearchBuilder;
import com.yahoo.searchdefinition.derived.DerivedConfiguration;
import com.yahoo.searchdefinition.derived.AttributeFields;
import com.yahoo.searchdefinition.derived.RawRankProfile;
import com.yahoo.searchdefinition.derived.TestableDeployLogger;
import com.yahoo.searchdefinition.parser.ParseException;
import ai.vespa.rankingexpression.importer.configmodelview.ImportedMlModels;
import com.yahoo.vespa.config.search.core.RankingExpressionsConfig;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class RankingExpressionsTestCase extends SchemaTestCase {

    @Test
    public void testFunctions() throws IOException, ParseException {
        RankProfileRegistry rankProfileRegistry = new RankProfileRegistry();
        Search search = SearchBuilder.createFromDirectory("src/test/examples/rankingexpressionfunction",
                                                          rankProfileRegistry,
                                                          new TestableDeployLogger()).getSearch();
        RankProfile functionsRankProfile = rankProfileRegistry.get(search, "macros");
        Map<String, RankProfile.RankingExpressionFunction> functions = functionsRankProfile.getFunctions();
        assertEquals(2, functions.get("titlematch$").function().arguments().size());
        assertEquals("var1", functions.get("titlematch$").function().arguments().get(0));
        assertEquals("var2", functions.get("titlematch$").function().arguments().get(1));
        assertEquals("var1 * var2 + 890", functions.get("titlematch$").function().getBody().getRoot().toString());
        assertEquals("0.8 + 0.2 * titlematch$(4,5) + 0.8 * titlematch$(7,8) * closeness(distance)",
                     functionsRankProfile.getFirstPhaseRanking().getRoot().toString());
        assertEquals("78 + closeness(distance)",
                     functions.get("artistmatch").function().getBody().getRoot().toString());
        assertEquals(0, functions.get("artistmatch").function().arguments().size());

        RawRankProfile rawRankProfile = new RawRankProfile(functionsRankProfile,
                                                           new QueryProfileRegistry(),
                                                           new ImportedMlModels(),
                                                           new AttributeFields(search));
        List<Pair<String, String>> rankProperties = rawRankProfile.configProperties();
        assertEquals(5, rankProperties.size());

        assertEquals("rankingExpression(titlematch$@126063073eb2deb.ab95cd69909927c).rankingScript", rankProperties.get(0).getFirst());
        assertEquals("4 * 5 + 890", rankProperties.get(0).getSecond());

        assertEquals("rankingExpression(titlematch$@c7e4c2d0e6d9f2a1.1d4ed08e56cce2e6).rankingScript", rankProperties.get(1).getFirst());
        assertEquals("7 * 8 + 890", rankProperties.get(1).getSecond());

        assertEquals("rankingExpression(artistmatch).rankingScript", rankProperties.get(2).getFirst());
        assertEquals("78 + closeness(distance)", rankProperties.get(2).getSecond());

        assertEquals("vespa.rank.firstphase", rankProperties.get(3).getFirst());
        assertEquals("rankingExpression(firstphase)", rankProperties.get(3).getSecond());

        assertEquals("rankingExpression(firstphase).rankingScript", rankProperties.get(4).getFirst());
        assertEquals("0.8 + 0.2 * rankingExpression(titlematch$@126063073eb2deb.ab95cd69909927c) + 0.8 * rankingExpression(titlematch$@c7e4c2d0e6d9f2a1.1d4ed08e56cce2e6) * closeness(distance)", rankProperties.get(4).getSecond());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testThatIncludingFileInSubdirFails() throws IOException, ParseException {
        RankProfileRegistry registry = new RankProfileRegistry();
        Search search = SearchBuilder.createFromDirectory("src/test/examples/rankingexpressioninfile",
                                                          registry,
                                                          new TestableDeployLogger()).getSearch();
        new DerivedConfiguration(search, new BaseDeployLogger(), new TestProperties(), registry, new QueryProfileRegistry(), new ImportedMlModels()); // rank profile parsing happens during deriving
    }

}
