using Test

import DataFrames: DataFrame
import PyFITS

tdir = mktempdir()

@testset "write_fits & read_fits" begin

    @testset "simple" begin
        df = DataFrame(
            a = [1, 2, 8],
            funky_key = [4.0, -π, NaN],
            wow = ["a", "b", "cd"],
        )
        df[!, Symbol("nasty test")] = [true, false, true]

        # write to fits
        PyFITS.write_fits(joinpath(tdir, "test.fits"), df)

        # read from fits
        df2 = PyFITS.read_fits(joinpath(tdir, "test.fits"))


        # check if the two dataframes are equal
        @test size(df) == size(df2)
        @test Set(names(df2)) == Set(names(df))
        @test df.a == df2.a
    end

    @testset "missings" begin
        df = DataFrame(
            a = [1, 2, missing],
            funky_key = [4.0, missing, NaN],
            wow = [missing, "b", "cd"],
        )
        df[!, Symbol("nasty test")] = [missing, missing, true]

        # write to fits
        PyFITS.write_fits(joinpath(tdir, "test.fits"), df)

        # read from fits
        df2 = PyFITS.read_fits(joinpath(tdir, "test.fits"))

        # check if the two dataframes are equal
        @test size(df) == size(df2)
        @test Set(names(df2)) == Set(names(df))
        @test df.a == df2.a
    end

    @testset "exceptions" begin 
        df = DataFrame(
            α = [1,2]
        )

        @test_throws ArgumentError PyFITS.write_fits(joinpath(tdir, "test.fits"), df)
    end

end

