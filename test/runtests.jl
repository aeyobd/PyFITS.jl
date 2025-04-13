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
            funky_key = [4.0, missing, 2.0],
            wow = [missing, "b", "cd"],
        )

        # write to fits
        PyFITS.write_fits(joinpath(tdir, "test.fits"), df, overwrite=true)

        # read from fits
        df2 = PyFITS.read_fits(joinpath(tdir, "test.fits"))

        # check if the two dataframes are equal
        @test size(df) == size(df2)
        @test Set(names(df2)) == Set(names(df))


        for col in names(df)
            for i in 1:size(df, 1)
                a = df[i, col]
                b = df2[i, col]
                if ismissing(a)
                    @test ismissing(b)
                elseif a isa Real
                    @test a ≈ b nans=true
                else
                    @test a == b
                end
            end
        end
    end

    @testset "exceptions" begin 
        df = DataFrame(
            α = [1,2]
        )

        @test_throws ArgumentError PyFITS.write_fits(joinpath(tdir, "test.fits"), df)
    end

end

