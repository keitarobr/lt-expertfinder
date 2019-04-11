package de.uhh.lt.xpertfinder.methods;

import de.uhh.lt.xpertfinder.finder.ExpertFindingResult;
import de.uhh.lt.xpertfinder.model.graph.Authorship;
import de.uhh.lt.xpertfinder.model.graph.Graph;
import de.uhh.lt.xpertfinder.finder.ExpertTopic;
import de.uhh.lt.xpertfinder.utils.MathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Service
public class InfiniteRandomMethod implements ExpertFindingMethod<InfiniteRandomMethod.InfiniteRandomRequest> {

    public class InfiniteRandomRequest extends DefaultRequest {
        private double lambda;
        private double epsilon;

        public InfiniteRandomRequest() {
        }

        public InfiniteRandomRequest(double lambda, double epsilon) {
            super();
            this.lambda = lambda;
            this.epsilon = epsilon;
        }

        public InfiniteRandomRequest(int documents, int results, double lambda, double epsilon) {
            super(documents, results);
            this.lambda = lambda;
            this.epsilon = epsilon;
        }

        public double getLambda() {
            return lambda;
        }

        public void setLambda(double lambda) {
            this.lambda = lambda;
        }

        public double getEpsilon() {
            return epsilon;
        }

        public void setEpsilon(double epsilon) {
            this.epsilon = epsilon;
        }
    }

    private static Logger logger = LoggerFactory.getLogger(InfiniteRandomMethod.class);

    @Override
    public String getId() {
        return "infiniterandom";
    }

    @Override
    public String getName() {
        return "Infinite Random Walk";
    }

    @Override
    public boolean needsCollaborations() {
        return false;
    }

    @Override
    public boolean needsCitations() {
        return false;
    }

    @Override
    public boolean needsPublications() {
        return true;
    }

    @Override
    public InfiniteRandomRequest getRequestObject() {
        return new InfiniteRandomRequest(1000, 25, 0.5, 0.00000008);
    }

    @Override
    public ExpertFindingResult findExperts(InfiniteRandomRequest request, ExpertTopic expertTopic) {
        double lambda = request.getLambda();
        double epsilon = request.getEpsilon();
        Graph graph = expertTopic.getGraph();
        Map<String, Double> documentRelevance = expertTopic.getDocumentRelevance();

        int maxIterations = 500;

        // create variables
        logger.debug("Init infinite random walk");
        Map<String, Double>[] pd = new Map[maxIterations];
        for(int i = 0; i < pd.length ; i++) {
            pd[i] = new HashMap<>();
        }
        Map<String, Double>[] pca = new Map[maxIterations];
        for(int i = 0; i < pd.length ; i++) {
            pca[i] = new HashMap<>();
        }
        // init variables t = 0
        pd[0] = documentRelevance;
        for(String author : graph.getAuthors()) {
            pca[0].put(author, 0d);
        }

        // calculate random walk
        logger.debug("Calculate infinite random walk");
        int i = 0;
        do {
            i++;

            for(String doc : graph.getDocs()) {
                double score = Math.exp( Math.log(lambda) + Math.log(documentRelevance.get(doc)));
                double score2 = 0;

                if(graph.getDocumentAuthorNeighbors().containsKey(doc)) {
                    for(Authorship authorship : graph.getDocumentAuthorNeighbors().get(doc)) {
                        score2 = score2 + Math.exp(
                                Math.log(pca[i-1].get(authorship.getAuthor()))
                                        + Math.log(1.0d / graph.getAuthorDocumentNeighbors().get(authorship.getAuthor()).size()));
                    }

                    pd[i].put(doc, score + Math.exp(Math.log(1 - lambda) + Math.log(score2)));
                } else {
                    pd[i].put(doc, score);
                }
            }

            for(String author : graph.getAuthors()) {
                double score = Math.exp(Math.log(lambda) + Math.log( graph.getAuthorDocumentNeighbors().containsKey(author) ? (double) graph.getAuthorDocumentNeighbors().get(author).size() / (double) graph.getDocs().size() : 0.0d));
                double score2 = 0;

                if(graph.getAuthorDocumentNeighbors().containsKey(author)) {
                    for(String document : graph.getAuthorDocumentNeighbors().get(author)) {
                        score2 = score2 + Math.exp(
                                Math.log(pd[i-1].get(document))
                                        + Math.log(1.0d / graph.getDocumentAuthorNeighbors().get(document).size())
                        );
                    }
                    pca[i].put(author, score + Math.exp(Math.log(1 - lambda) + Math.log(score2)));
                } else {
                    pca[i].put(author, score);
                }
            }

            if(i == maxIterations - 1) {
                break;
            }

        } while(!MathUtils.checkConvergence(pca[i], pca[i-1], epsilon));
        System.out.println(i + " Iterations");

        return new ExpertFindingResult(pd[i], pca[i]);
    }
}
