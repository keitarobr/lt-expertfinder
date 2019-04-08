package de.uhh.lt.xpertfinder.methods;

import de.uhh.lt.xpertfinder.finder.ExpertFindingResult;
import de.uhh.lt.xpertfinder.model.graph.Authorship;
import de.uhh.lt.xpertfinder.model.graph.Citation;
import de.uhh.lt.xpertfinder.model.graph.Collaboration;
import de.uhh.lt.xpertfinder.model.graph.Graph;
import de.uhh.lt.xpertfinder.service.ExpertTopic;
import de.uhh.lt.xpertfinder.utils.MathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Service
public class InfiniteRandomWeightedMethod implements ExpertFindingMethod {
    private static Logger logger = LoggerFactory.getLogger(InfiniteRandomWeightedMethod.class);

    private Graph graph;
    private Map<String, Double> documentRelevance;

    @Override
    public String getId() {
        return "infiniterandomweighted";
    }

    @Override
    public String getName() {
        return "Infinite Random Walk - Weighted Full Graph";
    }

    @Override
    public boolean needsCollaborations() {
        return true;
    }

    @Override
    public boolean needsCitations() {
        return true;
    }

    @Override
    public boolean needsPublications() {
        return true;
    }

    @Override
    public ExpertFindingResult findExperts(int k, double lambda, double epsilon, double md, double mca, ExpertTopic expertTopic) {
        graph = expertTopic.getGraph();
        documentRelevance = expertTopic.getDocumentRelevance();

        int maxIterations = 500;

        // create variables
        logger.debug("Init infinite random walk full weighted graph");
        Map<String, Double>[] pd = new Map[maxIterations];
        for(int i = 0; i < pd.length ; i++) {
            pd[i] = new HashMap<>();
        }
        Map<String, Double>[] pca = new Map[maxIterations];
        for(int i = 0; i < pd.length ; i++) {
            pca[i] = new HashMap<>();
        }

        // init variables t = 0
        // iteration 0
        int i = 0;
        pd[0] = documentRelevance;
        for(String author : graph.getAuthors()) {
            pca[0].put(author, 0d);
        }

        // calculate random walk
        logger.debug("Calculate infinite random walk full weighted graph");
        do {
            i++;

            for(String doc : graph.getDocs()) {
                double score = Math.exp( Math.log(lambda) + Math.log(pj(doc)));
                double score2 = 0;
                double score3 = 0;

                if(graph.getDocumentAuthorNeighbors().containsKey(doc)) {
                    for(Authorship authorship : graph.getDocumentAuthorNeighbors().get(doc)) {
                        score2 = score2 + Math.exp(
                                Math.log(pca2d(authorship.getAuthor(), doc))
                                        + Math.log(pca[i-1].get(authorship.getAuthor()))
                        );
                    }
                }

                if(graph.getDocumentDocumentInNeighbors().containsKey(doc)) {
                    for(String document : graph.getDocumentDocumentInNeighbors().get(doc)) {
                        score3 = score3 + Math.exp(
                                Math.log(pd2d(document, doc))
                                        + Math.log(pd[i-1].get(document))
                        );
                    }
                }

                //score + (1 - lambda) * ((1 - mu) * score2 + mu * score3)
                score = score + Math.exp(
                        Math.log(1 - lambda) + Math.log(
                                Math.exp(
                                        Math.log(1 - md) + Math.log(score2))
                                        + Math.exp(
                                        Math.log(md) + Math.log(score3)))
                );

                pd[i].put(doc, score);
            }

            for(String author : graph.getAuthors()) {
                double score = Math.exp(Math.log(lambda) + Math.log(pjca(author)));
                double score2 = 0;
                double score3 = 0;

                if(graph.getAuthorDocumentNeighbors().containsKey(author)) {
                    for(String document : graph.getAuthorDocumentNeighbors().get(author)) {
                        score2 = score2 + Math.exp(
                                Math.log(pd2ca(document, author))
                                        + Math.log(pd[i-1].get(document))
                        );
                    }
                }

                if(graph.getAuthorAuthorNeighbors2().containsKey(author)) {
                    for(Collaboration coll : graph.getAuthorAuthorNeighbors2().get(author)) {
                        score3 = score3 + Math.exp(
                                Math.log(pca2ca(coll.getAuthor(), author))
//                                        Math.log(coll.getWeight())
                                        + Math.log(pca[i-1].get(coll.getAuthor()))
                        );
                    }
                }

                score = score + Math.exp(
                        Math.log(1 - lambda) + Math.log(
                                Math.exp(
                                        Math.log(1 - mca) + Math.log(score2))
                                        + Math.exp(
                                        Math.log(mca) + Math.log(score3))
                        )
                );

                pca[i].put(author, score);
            }

            if(i == maxIterations - 1) {
                break;
            }

        } while(!MathUtils.checkConvergence(pca[i], pca[i-1], epsilon));
        System.out.println(i + " Iterations");

        return new ExpertFindingResult(pd[i], pca[i]);
    }

    // model 2 document relevance
    private double pj(String doc) {
        return documentRelevance.get(doc);
    }

    private double pca2d(String author, String doc) {
        return 1.0d / graph.getAuthorDocumentNeighbors().get(author).size();
    }

    // recency
    private double pd2d(String doc1, String doc2) {
        for (Citation citation : graph.getDocumentDocumentOutNeighbors().get(doc1)) {
            if(citation.getDocument().equals(doc2)) {
                return citation.getWeight();
            }
        }

        System.out.println("LOL WHY?!");
        return 0;
    }

    // written top docs / top docs
    private double pjca(String author) {
        return graph.getAuthorDocumentNeighbors().containsKey(author) ? (double) graph.getAuthorDocumentNeighbors().get(author).size() / (double) graph.getDocs().size() : 0.0d;
    }

    private double pd2ca(String document, String author) {
        for (Authorship authorship : graph.getDocumentAuthorNeighbors().get(document)) {
            if(authorship.getAuthor().equals(author)) {
                return authorship.getWeight();
            }
        }

        System.out.println("LOL WHY?!");
        return 0;
    }

    // local & global collaboration count
    private double pca2ca(String author1, String author2) {
        for (Collaboration collaboration : graph.getAuthorAuthorNeighbors2().get(author1)) {
            if(collaboration.getAuthor().equals(author2)) {
                return collaboration.getWeight();
            }
        }

        System.out.println("LOL WHY?!");
        return 0;
    }
}
