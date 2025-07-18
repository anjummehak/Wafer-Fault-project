import matplotlib.pyplot as plt
from sklearn.cluster import KMeans
from kneed import KneeLocator
from file_operations import file_methods

class KMeansClustering:
    """
    Class for performing KMeans clustering and saving results such as elbow plot and model.
    """

    def __init__(self, file_object, logger_object):
        """
        Initializes KMeansClustering with file and logger objects.

        Args:
            file_object: File object for logging.
            logger_object: Logger object to log messages.
        """
        self.file_object = file_object
        self.logger_object = logger_object

    def elbow_plot(self, data):
        """
        Generates and saves an elbow plot to determine the optimum number of clusters.

        Args:
            data: The dataset for clustering.

        Returns:
            The optimum number of clusters determined by the elbow method.
        
        Raises:
            Exception: If any error occurs during the plot generation.
        """
        self.logger_object.log(self.file_object, 'Entered the elbow_plot method of KMeansClustering')

        wcss = []
        try:
            # Iterating through possible cluster values from 1 to 10
            for i in range(1, 11):
                kmeans = KMeans(n_clusters=i, init='k-means++', random_state=42)
                kmeans.fit(data)
                wcss.append(kmeans.inertia_)

            # Plotting the elbow curve
            plt.plot(range(1, 11), wcss)
            plt.title('The Elbow Method')
            plt.xlabel('Number of clusters')
            plt.ylabel('WCSS')
            plt.savefig('preprocessing_data/K-Means_Elbow.PNG')  # Save plot locally

            # Finding optimum cluster using KneeLocator
            self.kn = KneeLocator(range(1, 11), wcss, curve='convex', direction='decreasing')
            self.logger_object.log(self.file_object, f'Optimum clusters: {self.kn.knee}. Exiting elbow_plot method.')

            return self.kn.knee
        except Exception as e:
            self.logger_object.log(self.file_object, f'Error in elbow_plot: {str(e)}')
            raise Exception('Error in elbow_plot method')

    def create_clusters(self, data, number_of_clusters):
        """
        Creates clusters using the KMeans algorithm and adds a new column with cluster information.

        Args:
            data: The dataset to be clustered.
            number_of_clusters: The number of clusters to create.

        Returns:
            A dataframe with an additional 'Cluster' column indicating the cluster each data point belongs to.

        Raises:
            Exception: If any error occurs during clustering or saving the model.
        """
        self.logger_object.log(self.file_object, 'Entered the create_clusters method of KMeansClustering')

        try:
            # Initializing and fitting the KMeans model
            kmeans = KMeans(n_clusters=number_of_clusters, init='k-means++', random_state=42)
            y_kmeans = kmeans.fit_predict(data)

            # Save the trained model
            file_op = file_methods.File_Operation(self.file_object, self.logger_object)
            file_op.save_model(kmeans, 'KMeans')

            # Add cluster labels to the data
            data['Cluster'] = y_kmeans

            self.logger_object.log(self.file_object, f'Successfully created {number_of_clusters} clusters. Exiting create_clusters method.')

            return data
        except Exception as e:
            self.logger_object.log(self.file_object, f'Error in create_clusters: {str(e)}')
            raise Exception('Error in create_clusters method')
