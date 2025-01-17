package org.epf.hadoop.colfil3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class RecommendationReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Map<String, Integer> recommendations = new HashMap<>();

        // j'accumule les scores pour chaque candidat
        for (Text value : values) {
            String[] parts = value.toString().split(":");
            String candidate = parts[0];
            int score = Integer.parseInt(parts[1]);

            recommendations.put(candidate, recommendations.getOrDefault(candidate, 0) + score);
        }

        // Trier les  score décroissant
        List<Map.Entry<String, Integer>> sortedRecommendations = new ArrayList<>(recommendations.entrySet());
        sortedRecommendations.sort((a, b) -> b.getValue().compareTo(a.getValue()));

        // Sélectionner les 5 meilleurs candidats
        StringBuilder topRecommendations = new StringBuilder();
        int count = 0;
        for (Map.Entry<String, Integer> entry : sortedRecommendations) {
            if (count >= 5) break;
            if (topRecommendations.length() > 0) {
                topRecommendations.append(", ");
            }
            topRecommendations.append(entry.getKey()).append(":").append(entry.getValue());
            count++;
        }

       
        context.write(key, new Text(topRecommendations.toString()));
    }
}
